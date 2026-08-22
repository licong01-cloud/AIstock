"""Run the approved P2-4 zero-fit untouched-holdout acceptance."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping

from psycopg2 import Error as PsycopgError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk.market_relative_ridge_holdout import (  # noqa: E402
    ACCEPTANCE_SCHEMA_VERSION,
    ALGORITHM_VERSION,
    CONTRACT_VERSION,
    HoldoutAcceptanceError,
    HOLDOUT_END,
    OUTCOME_TAIL_TRADING_DAYS,
    REASON_REPRODUCIBILITY,
    REASON_SOURCE,
    build_holdout_request,
    close_children,
    evaluate_child,
    expected_holdout_source,
    failure_receipt,
    finalize_acceptance,
    load_frozen_candidate,
    load_request,
    load_written_artifact,
    model_artifact,
    preflight_output,
    ready_artifact,
    validate_artifact_bundle,
    validate_output_identity,
    validate_static_request,
    write_once,
)
from backend.services.hmm_risk.state_model_set import (  # noqa: E402
    StateModelSetError,
    canonical_json_bytes,
    canonical_sha256,
)
from scripts.hmm_risk.prepare_state_model_set import (  # noqa: E402
    _connect_readonly,
    _load_l1_source_inputs,
    _require_database_identity_match,
)

SOURCE_LOADER_FAILURE = "hmm_risk_p2_4_source_loader_failed"


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


def _artifact_outputs(args: argparse.Namespace) -> dict[str, str]:
    child_1 = _child_path(args.child_dir, 1)
    child_2 = _child_path(args.child_dir, 2)
    return {
        "acceptance_output": str(args.output.resolve()),
        "acceptance_failure_output": str(_failure_path(args.output.resolve())),
        "model_output": str(args.model_output.resolve()),
        "ready_output": str(args.ready_output.resolve()),
        "child_1_output": str(child_1),
        "child_1_failure_output": str(_failure_path(child_1)),
        "child_2_output": str(child_2),
        "child_2_failure_output": str(_failure_path(child_2)),
    }


def _source_preflight_error(exc: Exception) -> HoldoutAcceptanceError:
    message = str(exc)
    candidate_reason = message.partition(":")[0].strip()
    source_reason_code = (
        candidate_reason
        if candidate_reason.startswith("hmm_risk_") and candidate_reason.replace("_", "").isalnum()
        else SOURCE_LOADER_FAILURE
    )
    return HoldoutAcceptanceError(
        REASON_SOURCE,
        message,
        stage="source_preflight",
        evidence={
            "exception_type": type(exc).__name__,
            "source_reason_code": source_reason_code,
            "error_message": message,
        },
    )


def _candidate_database_identity(candidate: Any) -> dict[str, Any]:
    value = candidate.report.get("database_identity")
    if not isinstance(value, Mapping):
        raise HoldoutAcceptanceError(
            REASON_SOURCE,
            "frozen candidate database identity is missing or invalid",
            stage="source_preflight",
        )
    return dict(value)


def _resolve_outcome_tail_end(
    db_prefix: str,
    *,
    expected_database_identity: Mapping[str, Any],
) -> date:
    try:
        conn, actual_database_identity = _connect_readonly(db_prefix)
        try:
            _require_database_identity_match(actual_database_identity, expected_database_identity)
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT cal_date::date
                    FROM market.trading_calendar
                    WHERE is_trading=true AND cal_date > %s
                    ORDER BY cal_date
                    LIMIT %s
                    """,
                    (HOLDOUT_END, OUTCOME_TAIL_TRADING_DAYS),
                )
                rows = cursor.fetchall()
        finally:
            try:
                conn.rollback()
            finally:
                conn.close()
    except HoldoutAcceptanceError:
        raise
    except (PsycopgError, StateModelSetError) as exc:
        raise _source_preflight_error(exc) from exc
    values = tuple(row[0] for row in rows)
    if (
        len(values) != OUTCOME_TAIL_TRADING_DAYS
        or any(type(value) is not date for value in values)
        or values != tuple(sorted(set(values)))
        or values[0] <= HOLDOUT_END
    ):
        raise HoldoutAcceptanceError(
            REASON_SOURCE,
            "canonical calendar cannot resolve the exact outcome-tail end",
            stage="source_preflight",
        )
    return values[-1]


def _load_holdout_inputs(
    request: dict[str, Any],
    *,
    db_prefix: str,
    expected_database_identity: Mapping[str, Any],
    source_preflight_complete: Callable[[], None],
) -> Any:
    try:
        return _load_l1_source_inputs(
            _loader_request(request),
            db_prefix=db_prefix,
            c010_formal=True,
            expected_database_identity=expected_database_identity,
            source_preflight_complete=source_preflight_complete,
        )
    except HoldoutAcceptanceError:
        raise
    except (PsycopgError, StateModelSetError) as exc:
        raise _source_preflight_error(exc) from exc


def _prepare_request(args: argparse.Namespace) -> int:
    request: dict[str, Any] = {}
    producer = "unknown"
    holdout_accessed = False
    try:
        candidate = load_frozen_candidate(args.candidate.resolve())
        expected_database_identity = _candidate_database_identity(candidate)
        producer = _producer_commit()
        outputs = _artifact_outputs(args)
        for path in (args.request.resolve(), *(Path(value) for value in outputs.values())):
            preflight_output(path, repository_root=ROOT)
        outcome_tail_end = _resolve_outcome_tail_end(
            str(args.db_env_prefix),
            expected_database_identity=expected_database_identity,
        )
        source = expected_holdout_source(outcome_tail_end=outcome_tail_end)

        def mark_holdout_accessed() -> None:
            nonlocal holdout_accessed
            holdout_accessed = True

        inputs = _load_holdout_inputs(
            {"holdout_source": {"source": source}},
            db_prefix=str(args.db_env_prefix),
            expected_database_identity=expected_database_identity,
            source_preflight_complete=mark_holdout_accessed,
        )
        request = build_holdout_request(
            inputs,
            candidate,
            source=source,
            artifact_outputs=outputs,
        )
        write_once(args.request.resolve(), request, repository_root=ROOT)
        sys.stdout.buffer.write(
            canonical_json_bytes(
                {
                    "status": "request_prepared",
                    "output": str(args.request.resolve()),
                    "request_sha256": request["request_sha256"],
                }
            )
            + b"\n"
        )
        return 0
    except Exception as exc:
        failure = failure_receipt(
            request=request,
            producer_commit=producer,
            error=exc,
            holdout_accessed=holdout_accessed,
            product_acceptance_performed=False,
        )
        try:
            write_once(_failure_path(args.output.resolve()), failure, repository_root=ROOT)
        except Exception as write_exc:
            sys.stderr.write(
                f"P2-4 request preparation failed and failure receipt could not be written: "
                f"{type(write_exc).__name__}: {write_exc}\n"
            )
            return 2
        sys.stderr.write(
            f"P2-4 request preparation failed: {failure['failure_reason_code']}; "
            f"receipt={_failure_path(args.output.resolve())}\n"
        )
        return 1


def _read_child(path: Path) -> dict[str, Any]:
    return load_written_artifact(
        path,
        label="P2-4 child receipt",
        reason="hmm_risk_p2_4_fresh_process_reproducibility_failed",
    )


def _read_child_failure(path: Path) -> dict[str, Any]:
    value = load_written_artifact(path, label="P2-4 child failure receipt")
    report_hash = str(value.get("report_sha256") or "")
    if canonical_sha256({key: item for key, item in value.items() if key != "report_sha256"}) != report_hash:
        raise HoldoutAcceptanceError(
            "hmm_risk_p2_4_readback_mismatch",
            "child failure receipt hash is invalid",
            stage="fresh_process",
        )
    return value


def _child_failure_error(
    child_failure: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    producer_commit: str,
    process_index: int,
    returncode: int,
    failure_path: Path,
) -> HoldoutAcceptanceError:
    reason = child_failure.get("failure_reason_code")
    stage = child_failure.get("failure_stage")
    failure_evidence = child_failure.get("failure_evidence")
    expected_identity = {
        "schema_version": ACCEPTANCE_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "status": "NOT_AVAILABLE",
        "producer_commit": producer_commit,
        "holdout_evaluation_id": request.get("holdout_evaluation_id"),
        "fit_count": 0,
        "selection_performed": False,
        "model_write": False,
        "model_sha256": None,
        "ready_write": False,
        "ready_sha256": None,
        "database_write": False,
        "runtime_action": False,
    }
    invalid_fields = sorted(
        field for field, expected in expected_identity.items() if child_failure.get(field) != expected
    )
    if not isinstance(reason, str) or not reason.startswith("hmm_risk_p2_4_"):
        invalid_fields.append("failure_reason_code")
    if not isinstance(stage, str) or not stage:
        invalid_fields.append("failure_stage")
    if not isinstance(failure_evidence, Mapping):
        invalid_fields.append("failure_evidence")
    for field in ("holdout_accessed", "product_acceptance_performed"):
        observed = child_failure.get(field)
        if field not in child_failure or (observed is not None and not isinstance(observed, bool)):
            invalid_fields.append(field)
    if child_failure.get("product_acceptance_performed") is True and child_failure.get("holdout_accessed") is not True:
        invalid_fields.append("product_acceptance_performed")
    if invalid_fields:
        raise HoldoutAcceptanceError(
            REASON_REPRODUCIBILITY,
            "child failure receipt identity is invalid",
            stage="fresh_process",
            evidence={
                "process_index": process_index,
                "returncode": returncode,
                "failure_receipt": str(failure_path),
                "invalid_fields": sorted(set(invalid_fields)),
            },
        )

    child_message = str(failure_evidence.get("error_message") or "P2-4 child process failed")
    return HoldoutAcceptanceError(
        reason,
        f"P2-4 child process failed: {child_message}",
        stage=stage,
        evidence={
            "process_index": process_index,
            "returncode": returncode,
            "failure_receipt": str(failure_path),
            "child_failure_reason_code": reason,
            "child_failure_stage": stage,
            "child_failure_evidence": dict(failure_evidence),
        },
    )


def _validate_cli_outputs(args: argparse.Namespace, request: dict[str, Any]) -> dict[str, str]:
    return validate_output_identity(
        request,
        acceptance_output=args.output.resolve(),
        model_output=args.model_output.resolve(),
        ready_output=args.ready_output.resolve(),
        child_1_output=_child_path(args.child_dir, 1),
        child_2_output=_child_path(args.child_dir, 2),
        repository_root=ROOT,
    )


def _merge_observed_flag(current: bool | None, observed: Any) -> bool | None:
    if current is True or observed is True:
        return True
    if current is None or observed is None:
        return None
    return False


def _child(args: argparse.Namespace) -> int:
    request: dict[str, Any] = {}
    producer = "unknown"
    holdout_accessed = False
    product_acceptance_performed = False
    output = _child_path(args.child_dir, int(args.child_index))
    try:
        request = load_request(args.request.resolve())
        candidate = load_frozen_candidate(args.candidate.resolve())
        expected_database_identity = _candidate_database_identity(candidate)
        validate_static_request(request, candidate)
        _validate_cli_outputs(args, request)
        producer = _producer_commit()

        def mark_holdout_accessed() -> None:
            nonlocal holdout_accessed
            holdout_accessed = True

        inputs = _load_holdout_inputs(
            request,
            db_prefix=str(args.db_env_prefix),
            expected_database_identity=expected_database_identity,
            source_preflight_complete=mark_holdout_accessed,
        )
        report = evaluate_child(
            inputs,
            request,
            candidate,
            process_index=int(args.child_index),
            producer_commit=producer,
        )
        product_acceptance_performed = True
        write_once(output, report, repository_root=ROOT)
        sys.stdout.buffer.write(canonical_json_bytes({"status": report["status"], "output": str(output)}) + b"\n")
        return 0
    except Exception as exc:
        failure = failure_receipt(
            request=request,
            producer_commit=producer,
            error=exc,
            holdout_accessed=holdout_accessed,
            product_acceptance_performed=product_acceptance_performed,
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
    product_acceptance_performed = False
    model_sha256: str | None = None
    ready_sha256: str | None = None
    try:
        request = load_request(args.request.resolve())
        candidate = load_frozen_candidate(args.candidate.resolve())
        validate_static_request(request, candidate)
        expected_outputs = _validate_cli_outputs(args, request)
        producer = _producer_commit()
        outputs = [Path(value) for value in expected_outputs.values()]
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
                child_failure_path = _failure_path(_child_path(args.child_dir, index))
                try:
                    child_failure = _read_child_failure(child_failure_path)
                    child_error = _child_failure_error(
                        child_failure,
                        request=request,
                        producer_commit=producer,
                        process_index=index,
                        returncode=completed.returncode,
                        failure_path=child_failure_path,
                    )
                except HoldoutAcceptanceError:
                    holdout_accessed = _merge_observed_flag(holdout_accessed, None)
                    product_acceptance_performed = _merge_observed_flag(product_acceptance_performed, None)
                    raise
                holdout_accessed = _merge_observed_flag(holdout_accessed, child_failure.get("holdout_accessed"))
                product_acceptance_performed = _merge_observed_flag(
                    product_acceptance_performed, child_failure.get("product_acceptance_performed")
                )
                raise child_error
            holdout_accessed = True
            product_acceptance_performed = True
        holdout_accessed = True
        first = _read_child(_child_path(args.child_dir, 1))
        second = _read_child(_child_path(args.child_dir, 2))
        draft = close_children(first, second, request=request, producer_commit=producer)
        product_acceptance_performed = True
        model: dict[str, Any] | None = None
        ready: dict[str, Any] | None = None
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
        acceptance_readback = load_written_artifact(args.output, label="P2-4 final acceptance")
        model_readback = (
            load_written_artifact(args.model_output, label="P2-4 canonical model") if model is not None else None
        )
        ready_readback = (
            load_written_artifact(args.ready_output, label="P2-4 READY marker") if ready is not None else None
        )
        validate_artifact_bundle(acceptance_readback, model=model_readback, ready=ready_readback)
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
            product_acceptance_performed=product_acceptance_performed,
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
    parser.add_argument("--prepare-request", action="store_true")
    parser.add_argument("--child-index", type=int, choices=(1, 2))
    args = parser.parse_args(argv)
    if args.prepare_request and args.child_index is not None:
        parser.error("--prepare-request and --child-index are mutually exclusive")
    if args.prepare_request:
        return _prepare_request(args)
    return _child(args) if args.child_index is not None else _parent(args)


if __name__ == "__main__":
    raise SystemExit(main())
