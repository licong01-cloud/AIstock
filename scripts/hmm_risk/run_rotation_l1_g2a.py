"""Build and execute the approved G2-A v1.2 development contract.

The parent mode launches the 15-fit battery and both 12-fit model processes as
fresh Python processes.  This CLI never reads the sealed tail, a database, or a
runtime service.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

for _name in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ[_name] = "1"

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.dataset_release.cas_store import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.rotation_l1_gbdt import (  # noqa: E402
    REASON_INPUT,
    REASON_REPRODUCIBILITY,
    RotationL1G2AError,
    canonical_sha256,
    close_processes,
    read_input_bundle,
    run_gbdt_process,
    run_ridge_battery,
    validate_battery_report,
    write_input_bundle,
)
from backend.services.hmm_risk.rotation_l1_input_bundle import (  # noqa: E402
    build_rotation_l1_inputs_from_assets,
)


def _load_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"JSON input cannot be read: {path}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON input must be an object: {path}")
    return value


def _write_once(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(value) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())


def _failure(
    error: BaseException,
    *,
    stage: str,
    fit_progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reason = str(getattr(error, "reason_code", REASON_INPUT))
    raw_evidence = getattr(error, "evidence", None)
    if not isinstance(raw_evidence, dict):
        raw_evidence = getattr(error, "context", None)
    evidence = raw_evidence if isinstance(raw_evidence, dict) else {"exception_type": type(error).__name__}
    body = {
        "schema_version": "hmm_risk_rotation_l1_g2a_failure_v1",
        "status": "failed",
        "stage": str(getattr(error, "stage", stage)),
        "reason_code": reason,
        "message": str(error),
        "evidence": evidence,
        "fit_progress": fit_progress or evidence.get("fit_progress"),
        "fit_success_claimed": False,
        "tail_accessed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "failure_sha256": canonical_sha256(body)}


def _parent_fit_progress(output: Path) -> dict[str, Any]:
    components: list[dict[str, Any]] = []
    for name in ("battery", "fresh_process_1", "fresh_process_2"):
        success_path = output / f"{name}.json"
        failure_path = output / f"{name}.failure.json"
        progress: Any = None
        status = "not_started"
        if success_path.exists():
            payload = _load_object(success_path)
            progress = (
                payload.get("fit_progress")
                if name == "battery"
                else (payload.get("reproducibility_payload") or {}).get("fit_progress")
            )
            status = "complete"
        elif failure_path.exists():
            payload = _load_object(failure_path)
            progress = payload.get("fit_progress")
            status = "failed"
        expected_planned = 15 if name == "battery" else 12
        if status == "not_started":
            progress = {
                "planned": expected_planned,
                "started": 0,
                "completed": 0,
                "failed": 0,
                "active_fit": None,
            }
            readback_valid = True
        else:
            readback_valid = isinstance(progress, dict) and set(progress) == {
                "planned",
                "started",
                "completed",
                "failed",
                "active_fit",
            }
        if readback_valid:
            readback_valid = (
                progress["planned"] == expected_planned
                and all(
                    isinstance(progress[field], int) and progress[field] >= 0
                    for field in ("started", "completed", "failed")
                )
                and progress["started"] <= progress["planned"]
                and progress["completed"] + progress["failed"] <= progress["started"]
                and (progress["active_fit"] is None or isinstance(progress["active_fit"], str))
            )
        if not readback_valid:
            progress = {
                "planned": expected_planned,
                "started": 0,
                "completed": 0,
                "failed": 0,
                "active_fit": None,
            }
        components.append({"component": name, "status": status, "readback_valid": readback_valid, **progress})
    body = {
        "planned": 39,
        "started": sum(int(item["started"]) for item in components),
        "completed": sum(int(item["completed"]) for item in components),
        "failed": sum(int(item["failed"]) for item in components),
        "active_fit": next((item["active_fit"] for item in components if item["active_fit"]), None),
        "components": components,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _ensure_external_new_directory(path: Path) -> Path:
    if not path.is_absolute():
        raise RuntimeError("output root must be absolute")
    output = path.parent.resolve(strict=True) / path.name
    try:
        output.relative_to(ROOT.resolve(strict=True))
    except ValueError:
        pass
    else:
        raise RuntimeError("output root must be outside the repository")
    output.mkdir(parents=False, exist_ok=False)
    return output


def _build_input(args: argparse.Namespace) -> int:
    authority = _load_object(args.industry_pit_authority)
    inputs, _source, _source_identity = build_rotation_l1_inputs_from_assets(
        direct_v2_candidate_root=args.candidate_root,
        security_identity_manifest=args.security_identity_manifest,
        provider_absence_manifest=args.provider_absence_manifest,
        industry_authority=authority,
        forbidden_roots=(ROOT,),
        work_parent=args.output_root.parent,
        g2a_contract=True,
    )
    g2a_bundle = inputs.get("g2a_bundle")
    if not isinstance(g2a_bundle, dict):
        raise RuntimeError("G2-A materialised bundle is missing")
    manifest = write_input_bundle(g2a_bundle, args.output_root, forbidden_roots=(ROOT,))
    print(json.dumps({"status": "complete", "manifest_sha256": manifest["manifest_sha256"]}, sort_keys=True))
    return 0


def _battery_child(args: argparse.Namespace) -> int:
    bundle = read_input_bundle(args.input_root, forbidden_roots=(ROOT,))["bundle"]
    _write_once(args.output_file, run_ridge_battery(bundle, producer_commit=args.producer_commit))
    return 0


def _model_child(args: argparse.Namespace) -> int:
    bundle = read_input_bundle(args.input_root, forbidden_roots=(ROOT,))["bundle"]
    battery = _load_object(args.battery_file)
    validate_battery_report(battery, expected_identity=bundle["identity"])
    if battery.get("producer_commit") != args.producer_commit:
        raise RotationL1G2AError(
            REASON_INPUT,
            "battery producer commit differs from the model request",
            stage="model-child",
        )
    report = run_gbdt_process(
        bundle,
        battery_report=battery,
        process_index=args.process_index,
    )
    _write_once(args.output_file, report)
    return 0


def _child_command(
    mode: str,
    *,
    input_root: Path,
    output_file: Path,
    producer_commit: str,
    extra: list[str] | None = None,
) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        mode,
        "--input-root",
        str(input_root),
        "--output-file",
        str(output_file),
        "--producer-commit",
        producer_commit,
        *(extra or []),
    ]


def _run_child(command: list[str], failure_path: Path) -> None:
    completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        if failure_path.exists():
            failure = _load_object(failure_path)
            failure_body = {key: value for key, value in failure.items() if key != "failure_sha256"}
            if (
                failure.get("schema_version") != "hmm_risk_rotation_l1_g2a_failure_v1"
                or failure.get("status") != "failed"
                or failure.get("failure_sha256") != canonical_sha256(failure_body)
            ):
                raise RotationL1G2AError(
                    REASON_REPRODUCIBILITY,
                    "fresh-process failure receipt identity differs",
                    stage="fresh_process_readback",
                )
            error = RotationL1G2AError(
                str(failure.get("reason_code") or REASON_INPUT),
                str(failure.get("message") or "fresh process failed"),
                stage=str(failure.get("stage") or "fresh_process"),
                evidence=failure.get("evidence") if isinstance(failure.get("evidence"), dict) else {},
            )
        else:
            detail = completed.stderr.strip()[-4000:]
            error = RuntimeError(f"fresh process failed with code {completed.returncode}: {detail}")
            _write_once(failure_path, _failure(error, stage="fresh_process"))
        raise error


def _run_parent(args: argparse.Namespace) -> int:
    output = _ensure_external_new_directory(args.output_root)
    battery_path = output / "battery.json"
    try:
        _run_child(
            _child_command(
                "battery-child",
                input_root=args.input_root,
                output_file=battery_path,
                producer_commit=args.producer_commit,
            ),
            output / "battery.failure.json",
        )
        child_paths = (output / "fresh_process_1.json", output / "fresh_process_2.json")
        for index, child_path in enumerate(child_paths, start=1):
            _run_child(
                _child_command(
                    "model-child",
                    input_root=args.input_root,
                    output_file=child_path,
                    producer_commit=args.producer_commit,
                    extra=["--battery-file", str(battery_path), "--process-index", str(index)],
                ),
                output / f"fresh_process_{index}.failure.json",
            )
        acceptance = close_processes(_load_object(child_paths[0]), _load_object(child_paths[1]))
        _write_once(output / "acceptance.json", acceptance)
    except Exception as exc:
        parent_failure = output / "parent.failure.json"
        if not parent_failure.exists():
            _write_once(
                parent_failure,
                _failure(exc, stage="parent", fit_progress=_parent_fit_progress(output)),
            )
        print(json.dumps({"status": "failed", "failure": str(parent_failure)}, sort_keys=True), file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": "development_complete",
                "output_root": str(output),
                "fit_count": 39,
                "acceptance_sha256": acceptance["acceptance_sha256"],
                "tail_accessed": False,
            },
            sort_keys=True,
        )
    )
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    build = subparsers.add_parser("build-input")
    build.add_argument("--candidate-root", type=Path, required=True)
    build.add_argument("--security-identity-manifest", type=Path, required=True)
    build.add_argument("--provider-absence-manifest", type=Path, required=True)
    build.add_argument("--industry-pit-authority", type=Path, required=True)
    build.add_argument("--output-root", type=Path, required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--input-root", type=Path, required=True)
    run.add_argument("--output-root", type=Path, required=True)
    run.add_argument("--producer-commit", required=True)
    battery = subparsers.add_parser("battery-child")
    battery.add_argument("--input-root", type=Path, required=True)
    battery.add_argument("--output-file", type=Path, required=True)
    battery.add_argument("--producer-commit", required=True)
    child = subparsers.add_parser("model-child")
    child.add_argument("--input-root", type=Path, required=True)
    child.add_argument("--output-file", type=Path, required=True)
    child.add_argument("--battery-file", type=Path, required=True)
    child.add_argument("--process-index", type=int, choices=(1, 2), required=True)
    child.add_argument("--producer-commit", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.mode == "build-input":
            return _build_input(args)
        if args.mode == "battery-child":
            return _battery_child(args)
        if args.mode == "model-child":
            return _model_child(args)
        return _run_parent(args)
    except Exception as exc:
        if args.mode in {"battery-child", "model-child"}:
            failure_path = args.output_file.with_name(f"{args.output_file.stem}.failure.json")
            if not failure_path.exists():
                _write_once(failure_path, _failure(exc, stage=args.mode))
        elif args.mode == "build-input":
            failure_path = args.output_root.parent / f"{args.output_root.name}.failure.json"
            if args.output_root.parent.exists() and not failure_path.exists():
                _write_once(failure_path, _failure(exc, stage=args.mode))
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason_code": getattr(exc, "reason_code", REASON_INPUT),
                    "message": str(exc),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
