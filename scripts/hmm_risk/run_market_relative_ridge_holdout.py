"""Run the approved P2-4 zero-fit untouched-holdout acceptance."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk.market_relative_ridge_holdout import (  # noqa: E402
    HoldoutAcceptanceError,
    close_children,
    evaluate_child,
    failure_receipt,
    finalize_acceptance,
    load_frozen_candidate,
    load_request,
    model_artifact,
    preflight_output,
    ready_artifact,
    validate_static_request,
    write_once,
)
from backend.services.hmm_risk.state_model_set import canonical_json_bytes  # noqa: E402
from scripts.hmm_risk.prepare_state_model_set import _load_l1_source_inputs  # noqa: E402


def _producer_commit() -> str:
    status = subprocess.run(
        ["git", "-C", str(ROOT), "status", "--porcelain"],
        check=True,
        capture_output=True,
        text=True,
    )
    if status.stdout.strip():
        raise HoldoutAcceptanceError(
            "hmm_risk_p2_4_request_identity_mismatch",
            "producer worktree must be clean",
            stage="preflight",
        )
    return subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _failure_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}.failure.json")


def _child_path(directory: Path, index: int) -> Path:
    return directory.resolve() / f"p2_4_holdout_child_{index}.json"


def _loader_request(request: dict[str, Any]) -> dict[str, Any]:
    source = request["holdout_source"]["source"]
    family = {"train_start": "2022-01-04", "train_end": "2025-03-31"}
    return {"source": source, "families": [dict(family), dict(family)]}


def _read_child(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise HoldoutAcceptanceError(
            "hmm_risk_p2_4_fresh_process_reproducibility_failed",
            "child receipt cannot be read",
            stage="parent_closure",
            evidence={"path": str(path), "exception_type": type(exc).__name__},
        ) from exc
    if not isinstance(value, dict):
        raise HoldoutAcceptanceError(
            "hmm_risk_p2_4_fresh_process_reproducibility_failed",
            "child receipt must be an object",
            stage="parent_closure",
        )
    return value


def _child(args: argparse.Namespace) -> int:
    request: dict[str, Any] = {}
    producer = "unknown"
    holdout_accessed = False
    output = _child_path(args.child_dir, int(args.child_index))
    try:
        request = load_request(args.request.resolve())
        candidate = load_frozen_candidate(args.candidate.resolve())
        validate_static_request(request, candidate)
        producer = _producer_commit()
        inputs = _load_l1_source_inputs(
            _loader_request(request),
            db_prefix=str(args.db_env_prefix),
            c010_formal=True,
        )
        holdout_accessed = True
        report = evaluate_child(
            inputs,
            request,
            candidate,
            process_index=int(args.child_index),
            producer_commit=producer,
        )
        write_once(output, report, repository_root=ROOT)
        sys.stdout.buffer.write(canonical_json_bytes({"status": report["status"], "output": str(output)}) + b"\n")
        return 0
    except Exception as exc:
        failure = failure_receipt(
            request=request,
            producer_commit=producer,
            error=exc,
            holdout_accessed=holdout_accessed,
        )
        try:
            write_once(_failure_path(output), failure, repository_root=ROOT)
        except Exception as write_exc:
            sys.stderr.write(
                f"P2-4 child failed and failure receipt could not be written: {type(write_exc).__name__}: {write_exc}\n"
            )
            return 2
        sys.stderr.write(f"P2-4 child failed: {failure['failure_reason_code']}; receipt={_failure_path(output)}\n")
        return 1


def _parent(args: argparse.Namespace) -> int:
    request: dict[str, Any] = {}
    producer = "unknown"
    holdout_accessed = False
    model_sha256: str | None = None
    ready_sha256: str | None = None
    try:
        request = load_request(args.request.resolve())
        candidate = load_frozen_candidate(args.candidate.resolve())
        validate_static_request(request, candidate)
        producer = _producer_commit()
        outputs = [args.output.resolve(), args.model_output.resolve(), args.ready_output.resolve()]
        outputs.extend(_child_path(args.child_dir, index) for index in (1, 2))
        outputs.extend(_failure_path(path) for path in list(outputs))
        if len(set(outputs)) != len(outputs):
            raise HoldoutAcceptanceError(
                "hmm_risk_p2_4_output_collision",
                "P2-4 output identities collide",
                stage="preflight",
            )
        for path in outputs:
            preflight_output(path, repository_root=ROOT)
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
            "--request",
            str(args.request.resolve()),
            "--candidate",
            str(args.candidate.resolve()),
            "--output",
            str(args.output.resolve()),
            "--model-output",
            str(args.model_output.resolve()),
            "--ready-output",
            str(args.ready_output.resolve()),
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
                holdout_accessed = True
                raise HoldoutAcceptanceError(
                    "hmm_risk_p2_4_fresh_process_reproducibility_failed",
                    "P2-4 child process failed",
                    stage="fresh_process",
                    evidence={
                        "process_index": index,
                        "returncode": completed.returncode,
                        "failure_receipt": str(_failure_path(_child_path(args.child_dir, index))),
                    },
                )
        holdout_accessed = True
        first = _read_child(_child_path(args.child_dir, 1))
        second = _read_child(_child_path(args.child_dir, 2))
        draft = close_children(first, second, request=request, producer_commit=producer)
        if draft["status"] in {"FULL_READY", "COVERAGE_AVAILABLE"}:
            model = model_artifact(draft, candidate)
            write_once(args.model_output, model, repository_root=ROOT)
            model_sha256 = str(model["model_sha256"])
            if draft["status"] == "FULL_READY":
                ready = ready_artifact(draft, model)
                write_once(args.ready_output, ready, repository_root=ROOT)
                ready_sha256 = str(ready["ready_sha256"])
        acceptance = finalize_acceptance(draft, model_sha256=model_sha256, ready_sha256=ready_sha256)
        write_once(args.output, acceptance, repository_root=ROOT)
        sys.stdout.buffer.write(
            canonical_json_bytes({"status": acceptance["status"], "output": str(args.output.resolve())}) + b"\n"
        )
        return 0
    except Exception as exc:
        failure = failure_receipt(
            request=request,
            producer_commit=producer,
            error=exc,
            holdout_accessed=holdout_accessed,
            model_sha256=model_sha256,
            ready_sha256=ready_sha256,
        )
        try:
            write_once(_failure_path(args.output), failure, repository_root=ROOT)
        except Exception as write_exc:
            sys.stderr.write(
                f"P2-4 parent failed and failure receipt could not be written: "
                f"{type(write_exc).__name__}: {write_exc}\n"
            )
            return 2
        sys.stderr.write(
            f"P2-4 parent failed: {failure['failure_reason_code']}; receipt={_failure_path(args.output)}\n"
        )
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model-output", type=Path, required=True)
    parser.add_argument("--ready-output", type=Path, required=True)
    parser.add_argument("--child-dir", type=Path, required=True)
    parser.add_argument("--db-env-prefix", required=True)
    parser.add_argument("--child-index", type=int, choices=(1, 2))
    args = parser.parse_args(argv)
    return _child(args) if args.child_index is not None else _parent(args)


if __name__ == "__main__":
    raise SystemExit(main())
