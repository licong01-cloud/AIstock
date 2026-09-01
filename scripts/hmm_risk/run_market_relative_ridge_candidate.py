"""Run an approved P2-3B or P2-3C Ridge candidate on read-only inputs."""

from __future__ import annotations

import argparse
import hashlib
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
    RL1_PROCESS_FIT_COUNT,
    RL1_REQUEST_SCHEMA_VERSION,
    RL1_REPORT_SCHEMA_VERSION,
    RL1_SOURCE_REVISION,
    REQUEST_SCHEMA_VERSION,
    REASON_HOLDOUT,
    REASON_INPUT_IDENTITY,
    REASON_RL1_INPUT,
    RidgeCandidateError,
    build_c012_rl1_capability_bundle,
    build_c012_rl1_component_model,
    build_c012_rl1_replay_request,
    canonical_json_bytes,
    canonical_sha256,
    close_c012_rl1_candidate_children,
    failure_report,
    finalize_c012_rl1_replay_acceptance,
    preflight_output_path,
    report_for_write,
    run_p2_3b_candidate,
    run_p2_3c_candidate,
    run_c012_rl1_candidate_process,
    validate_c012_rl1_replay_artifacts,
    validate_c012_rl1_static_request,
    validate_p2_3c_static_request,
    write_report,
)
from backend.services.hmm_risk.industry_pit_adapter import (  # noqa: E402
    HMM_G2A_DATA_A_CONTRACT_VERSION,
    HMM_INDUSTRY_PIT_AUTHORITY_SCHEMA,
    HMM_INDUSTRY_RESEARCH_BASIS_SCHEMA,
    HMM_L1_CODE_PROJECTION_SCHEMA,
    HMM_L1_CODE_PROJECTION_VERSION,
)
from backend.services.canonical_equity_pit import (  # noqa: E402
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_UNIVERSE_KEY,
)
from backend.services.hmm_risk.state_model_set import StateModelSetError  # noqa: E402
from backend.services.hmm_risk.rotation_l1_input_bundle import (  # noqa: E402
    RotationL1InputBundleError,
    read_rotation_l1_input_bundle,
)
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
    return {
        "acceptance_core_path": str(args.acceptance_core_output.resolve()),
        "acceptance_path": str(args.output.resolve()),
        "component_model_path": str(args.model_output.resolve()),
        "capability_bundle_path": str(args.bundle_output.resolve()),
        "child_dir": str(args.child_dir.resolve()),
        "failure_path": str(_failure_path(args.output.resolve())),
    }


def _source_authority(path: Path) -> dict[str, Any]:
    value = _load_request(path)
    if value.get("schema_version") == "pit_v2_source_freeze_receipt_v2":
        profiles = value.get("profiles")
        profile = profiles.get("canonical_v2") if isinstance(profiles, Mapping) else None
        if not isinstance(profile, Mapping):
            raise RidgeCandidateError(
                REASON_RL1_INPUT,
                "canonical PIT v2 source profile is missing",
                stage="request_preparation",
            )
        config_path = str(profile.get("path") or "")
        if config_path != "configs/datasets/qe_backtest_monthly_v2.yaml":
            raise RidgeCandidateError(
                REASON_RL1_INPUT,
                "canonical PIT v2 source profile path drifted",
                stage="request_preparation",
            )
        config = (ROOT / config_path).resolve()
        try:
            config.relative_to(ROOT)
        except ValueError as exc:
            raise RidgeCandidateError(
                REASON_RL1_INPUT,
                "canonical PIT v2 source profile escapes the repository",
                stage="request_preparation",
            ) from exc
        if (
            not config.is_file()
            or hashlib.sha256(config.read_bytes()).hexdigest() != profile.get("file_sha256")
            or profile.get("universe_key") != CANONICAL_PIT_UNIVERSE_KEY
            or profile.get("rule_version") != CANONICAL_PIT_RULE_VERSION
            or profile.get("declared_target_authority_status") != "ACTIVE_CANONICAL"
        ):
            raise RidgeCandidateError(
                REASON_RL1_INPUT,
                "canonical PIT v2 source profile identity is invalid",
                stage="request_preparation",
            )
        security_path = "backend/services/hmm_risk/manifests/security_source_identity_v1.json"
        absence_path = "backend/services/hmm_risk/manifests/provider_absence_v1.json"
        return {
            "source_start": "2020-07-30",
            "circ_mv_history_start": "2020-07-30",
            "source_end": RL1_DEVELOPMENT_END.isoformat(),
            "universe_key": CANONICAL_PIT_UNIVERSE_KEY,
            "universe_rule_version": CANONICAL_PIT_RULE_VERSION,
            "security_identity_manifest_path": security_path,
            "security_identity_manifest_sha256": canonical_sha256(_load_request(ROOT / security_path)),
            "provider_absence_manifest_path": absence_path,
            "provider_absence_manifest_sha256": canonical_sha256(_load_request(ROOT / absence_path)),
        }
    source = value.get("source") if "source" in value else value
    if not isinstance(source, dict):
        raise RidgeCandidateError(REASON_RL1_INPUT, "source authority is missing", stage="request_preparation")
    required = {
        "source_start",
        "circ_mv_history_start",
        "universe_key",
        "universe_rule_version",
        "security_identity_manifest_path",
        "security_identity_manifest_sha256",
        "provider_absence_manifest_path",
        "provider_absence_manifest_sha256",
    }
    if set(source) != required | {"source_end"}:
        raise RidgeCandidateError(REASON_RL1_INPUT, "source authority is incomplete", stage="request_preparation")
    return dict(source)


def _industry_pit_authority(path: Path) -> dict[str, Any]:
    value = _load_request(path)
    if set(value) != {"artifact_root", "identity", "l1_projection", "research_basis"}:
        raise RidgeCandidateError(
            REASON_RL1_INPUT,
            "industry PIT authority is incomplete",
            stage="request_preparation",
        )
    artifact_root = Path(str(value.get("artifact_root") or ""))
    identity = value.get("identity")
    l1_projection = value.get("l1_projection")
    research_basis = value.get("research_basis")
    expected_identity_keys = {
        "schema_version",
        "bundle_hash",
        "classification_candidate_hash",
        "index_membership_candidate_hash",
        "classification_receipt_hash",
        "index_membership_receipt_hash",
        "preflight_canonical_hash",
    }
    if (
        not artifact_root.is_absolute()
        or not isinstance(identity, Mapping)
        or set(identity) != expected_identity_keys
        or identity.get("schema_version") != HMM_INDUSTRY_PIT_AUTHORITY_SCHEMA
        or not isinstance(l1_projection, Mapping)
        or l1_projection.get("schema_version") != HMM_L1_CODE_PROJECTION_SCHEMA
        or l1_projection.get("projection_version") != HMM_L1_CODE_PROJECTION_VERSION
        or not isinstance(research_basis, Mapping)
        or set(research_basis)
        != {
            "schema_version",
            "contract_version",
            "active_mode",
            "historical_classification_basis",
            "historical_non_as_known_taxonomy",
            "forward_classification_basis",
            "forward_non_as_known_taxonomy",
            "canonical_hash",
        }
        or research_basis.get("schema_version") != HMM_INDUSTRY_RESEARCH_BASIS_SCHEMA
        or research_basis.get("contract_version") != HMM_G2A_DATA_A_CONTRACT_VERSION
    ):
        raise RidgeCandidateError(
            REASON_RL1_INPUT,
            "industry PIT authority root/identity is invalid",
            stage="request_preparation",
        )
    return {
        "artifact_root": str(artifact_root.resolve()),
        "identity": dict(identity),
        "l1_projection": dict(l1_projection),
        "research_basis": dict(research_basis),
    }


def _validate_c012_cli_output_scope(args: argparse.Namespace) -> None:
    root = args.output.resolve().parent
    file_paths = (
        args.request.resolve(),
        args.output.resolve(),
        args.acceptance_core_output.resolve(),
        args.model_output.resolve(),
        args.bundle_output.resolve(),
        _failure_path(args.output.resolve()),
    )
    if any(path.parent != root for path in file_paths):
        raise RidgeCandidateError(
            REASON_RL1_INPUT,
            "rotation L1 request and artifact files must share one artifact root",
            stage="output_preflight",
        )
    try:
        args.child_dir.resolve().relative_to(root)
    except ValueError as exc:
        raise RidgeCandidateError(
            REASON_RL1_INPUT,
            "rotation L1 child directory escapes the artifact root",
            stage="output_preflight",
        ) from exc


def _load_c012_input_bundle(path: Path) -> dict[str, Any]:
    if not path.is_absolute():
        raise RidgeCandidateError(
            REASON_RL1_INPUT,
            "rotation L1 input bundle root must be absolute",
            stage="source_preflight",
        )
    try:
        return read_rotation_l1_input_bundle(path.resolve(), forbidden_roots=(ROOT,))
    except RotationL1InputBundleError as exc:
        raise RidgeCandidateError(
            REASON_RL1_INPUT,
            str(exc),
            stage="source_preflight",
            evidence={"input_bundle_reason_code": exc.reason_code, "input_bundle_context": exc.context},
        ) from exc


def _prepare_c012_rl1_request(args: argparse.Namespace) -> int:
    producer_commit = "unknown"
    request: dict[str, Any] = {}
    failure_output = _failure_path(args.output.resolve())
    try:
        producer_commit = _producer_commit()
        _validate_c012_cli_output_scope(args)
        outputs = _c012_artifact_outputs(args)
        for path in (
            args.request.resolve(),
            args.output.resolve(),
            args.acceptance_core_output.resolve(),
            args.model_output.resolve(),
            args.bundle_output.resolve(),
            _child_path(args.child_dir, 1),
            _child_path(args.child_dir, 2),
        ):
            preflight_output_path(path, repository_root=ROOT)
        inputs = _load_c012_input_bundle(args.input_bundle_root)
        source = dict(inputs["source"])
        if source.get("source_revision") != RL1_SOURCE_REVISION:
            raise RidgeCandidateError(
                REASON_RL1_INPUT,
                "rotation L1 input bundle source revision differs",
                stage="source_preflight",
            )
        request = build_c012_rl1_replay_request(
            inputs,
            source=source,
            outputs=outputs,
            producer_commit=producer_commit,
        )
        write_report(args.request.resolve(), request, repository_root=ROOT)
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
        completed = int(exc.evidence.get("completed_fit_count") or 0) if isinstance(exc, RidgeCandidateError) else 0
        report = report_for_write(
            failure_report(
                request,
                producer_commit=producer_commit,
                error=exc,
                completed_fit_count=completed,
                candidate_mode="c012-rl1",
            ),
            failure=True,
        )
        try:
            write_report(failure_output, report, repository_root=ROOT)
        except Exception as write_exc:
            sys.stderr.write(
                "c012-rl1 request preparation failed and failure receipt could not be written: "
                f"{type(write_exc).__name__}: {write_exc}\n"
            )
            return 2
        sys.stderr.write(f"c012-rl1 request preparation failed: {report['failure_reason_code']}\n")
        return 1


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
    acceptance_core_written = False
    model_written = False
    bundle_written = False
    completed_fit_count = 0
    try:
        output = preflight_output_path(output, repository_root=ROOT)
        request = _load_request(args.request.resolve())
        validate_c012_rl1_static_request(request)
        failure_output = _failure_path(output) if args.child_index else Path(str(request["outputs"]["failure_path"]))
        _validate_c012_cli_output_scope(args)
        if request.get("outputs") != _c012_artifact_outputs(args):
            raise RidgeCandidateError(
                REASON_RL1_INPUT,
                "rotation L1 CLI outputs differ from request authority",
                stage="input",
            )
        for authorized_path in (
            args.acceptance_core_output.resolve(),
            args.model_output.resolve(),
            args.bundle_output.resolve(),
            args.output.resolve(),
        ):
            preflight_output_path(authorized_path, repository_root=ROOT)
        producer_commit = _producer_commit()
        if request.get("expected_producer_commit") != producer_commit:
            raise RidgeCandidateError(
                REASON_RL1_INPUT,
                "rotation L1 producer commit differs from request authority",
                stage="input",
            )
        if args.child_index is not None:
            inputs = _load_c012_input_bundle(args.input_bundle_root)
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
        for path in (
            *child_paths,
            args.acceptance_core_output.resolve(),
            args.model_output.resolve(),
            args.bundle_output.resolve(),
        ):
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
            "--acceptance-core-output",
            str(args.acceptance_core_output.resolve()),
            "--model-output",
            str(args.model_output.resolve()),
            "--bundle-output",
            str(args.bundle_output.resolve()),
            "--input-bundle-root",
            str(args.input_bundle_root.resolve()),
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
                        "completed_fit_count": completed_fit_count + int(child_failure.get("completed_fit_count") or 0),
                        "child_failure_receipt": str(failure_path),
                        "child_failure_report_sha256": child_failure.get("report_sha256"),
                        "child_failure_evidence": dict(child_failure.get("failure_evidence") or {}),
                    },
                )
            completed_fit_count += RL1_PROCESS_FIT_COUNT
        first, second = (_load_request(path) for path in child_paths)
        acceptance_core = close_c012_rl1_candidate_children(
            first,
            second,
            request=request,
            producer_commit=producer_commit,
        )
        component_model = build_c012_rl1_component_model(acceptance_core)
        capability_bundle = build_c012_rl1_capability_bundle(acceptance_core, component_model)
        final_acceptance = finalize_c012_rl1_replay_acceptance(
            acceptance_core,
            component_model,
            capability_bundle,
        )
        write_report(args.acceptance_core_output.resolve(), acceptance_core, repository_root=ROOT)
        acceptance_core_written = True
        write_report(args.model_output.resolve(), component_model, repository_root=ROOT)
        model_written = True
        write_report(args.bundle_output.resolve(), capability_bundle, repository_root=ROOT)
        bundle_written = True
        write_report(output, final_acceptance, repository_root=ROOT)
        validate_c012_rl1_replay_artifacts(
            _load_request(args.acceptance_core_output.resolve()),
            _load_request(args.model_output.resolve()),
            _load_request(args.bundle_output.resolve()),
            _load_request(output),
        )
        sys.stdout.buffer.write(
            canonical_json_bytes({"status": final_acceptance["status"], "output": str(output)}) + b"\n"
        )
        return 0
    except Exception as exc:
        completed = completed_fit_count
        if isinstance(exc, RidgeCandidateError):
            completed = max(completed, int(exc.evidence.get("completed_fit_count") or 0))
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
        if args.child_index is None and (acceptance_core_written or model_written or bundle_written):
            body = {key: value for key, value in report.items() if key != "report_sha256"}
            prior_evidence = body.get("failure_evidence")
            body["failure_evidence"] = {
                **(dict(prior_evidence) if isinstance(prior_evidence, Mapping) else {}),
                "partial_artifact_writes": {
                    "acceptance_core_write": acceptance_core_written,
                    "model_write": model_written,
                    "bundle_write": bundle_written,
                },
            }
            body["candidate_receipt_write"] = acceptance_core_written
            body["model_write"] = model_written
            body["bundle_write"] = bundle_written
            report = {**body, "report_sha256": canonical_sha256(body)}
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
    parser.add_argument("--db-env-prefix")
    parser.add_argument("--input-bundle-root", type=Path)
    parser.add_argument("--child-dir", type=Path)
    parser.add_argument("--child-index", type=int, choices=(1, 2))
    parser.add_argument("--acceptance-core-output", type=Path)
    parser.add_argument("--model-output", type=Path)
    parser.add_argument("--bundle-output", type=Path)
    parser.add_argument("--prepare-request", action="store_true")
    parser.add_argument("--source-authority", type=Path)
    parser.add_argument("--industry-pit-authority", type=Path)
    args = parser.parse_args(argv)

    if args.candidate_mode == "c012-rl1":
        required = {
            "--child-dir": args.child_dir,
            "--acceptance-core-output": args.acceptance_core_output,
            "--model-output": args.model_output,
            "--bundle-output": args.bundle_output,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            parser.error(f"{', '.join(missing)} required for c012-rl1")
        if args.input_bundle_root is None:
            parser.error("--input-bundle-root is required for c012-rl1")
        if args.db_env_prefix is not None:
            parser.error("--db-env-prefix is forbidden for c012-rl1 input-bundle execution")
        if args.source_authority is not None or args.industry_pit_authority is not None:
            parser.error("legacy source authority arguments are forbidden for c012-rl1 input-bundle execution")
        if args.prepare_request:
            if args.child_index is not None:
                parser.error("--prepare-request and --child-index are mutually exclusive")
            return _prepare_c012_rl1_request(args)
        return _run_c012_rl1(args)
    if args.db_env_prefix is None:
        parser.error("--db-env-prefix is required for p2-3b/p2-3c legacy execution")
    if (
        any(
            value is not None
            for value in (
                args.child_dir,
                args.child_index,
                args.acceptance_core_output,
                args.model_output,
                args.bundle_output,
                args.source_authority,
                args.industry_pit_authority,
                args.input_bundle_root,
            )
        )
        or args.prepare_request
    ):
        parser.error("c012-rl1-only arguments are not valid for other candidate modes")

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
