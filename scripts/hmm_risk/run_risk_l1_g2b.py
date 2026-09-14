"""Build and execute the approved G2-B risk-L1 development contract."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.dataset_release.cas_store import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.risk_l1_g2b import (  # noqa: E402
    CONTRACT_VERSION,
    REASON_INPUT,
    REASON_READBACK,
    REASON_REPRODUCIBILITY,
    RiskL1G2BError,
    close_processes,
    read_input_bundle,
    run_process,
    write_input_bundle,
)
from backend.services.hmm_risk.rotation_l1_input_bundle import (  # noqa: E402
    build_rotation_l1_inputs_from_assets,
)
from backend.services.hmm_risk.state_model_set import canonical_sha256  # noqa: E402


def _load_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RiskL1G2BError(REASON_INPUT, f"cannot read {path}", stage="readback") from exc
    if not isinstance(value, dict):
        raise RiskL1G2BError(REASON_INPUT, f"{path} is not an object", stage="readback")
    return value


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes(dict(value)) + b"\n"
    try:
        with path.open("xb") as handle:
            handle.write(payload)
    except FileExistsError as exc:
        raise RiskL1G2BError("hmm_risk_risk_l1_output_collision", f"output already exists: {path}", stage="writer") from exc


def _failure(error: BaseException, *, stage: str) -> dict[str, Any]:
    body = {
        "schema_version": "hmm_risk_risk_l1_g2b_failure_v1",
        "contract_version": CONTRACT_VERSION,
        "status": "failed",
        "reason_code": str(getattr(error, "reason_code", "hmm_risk_risk_l1_unknown_failure")),
        "stage": str(getattr(error, "stage", stage)),
        "message": str(error),
        "evidence": dict(getattr(error, "evidence", {}) or {}),
        "tail_accessed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "failure_sha256": canonical_sha256(body)}


def _external_new_directory(path: Path) -> Path:
    if not path.is_absolute():
        raise RiskL1G2BError(REASON_INPUT, "output root must be absolute", stage="writer")
    output = path.parent.resolve(strict=True) / path.name
    try:
        output.relative_to(ROOT.resolve(strict=True))
    except ValueError:
        pass
    else:
        raise RiskL1G2BError(REASON_INPUT, "output root cannot be inside repository", stage="writer")
    try:
        output.mkdir(parents=False, exist_ok=False)
    except FileExistsError as exc:
        raise RiskL1G2BError(
            "hmm_risk_risk_l1_output_collision",
            f"output root already exists: {output}",
            stage="writer",
        ) from exc
    except OSError as exc:
        raise RiskL1G2BError(REASON_INPUT, f"cannot create output root: {output}", stage="writer") from exc
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
        risk_l1_contract=True,
    )
    bundle = inputs.get("risk_l1_bundle")
    if not isinstance(bundle, dict):
        raise RiskL1G2BError(REASON_INPUT, "risk materialised bundle is missing", stage="build_input")
    manifest = write_input_bundle(bundle, args.output_root, forbidden_roots=(ROOT,))
    print(json.dumps({"status": "complete", "manifest_sha256": manifest["manifest_sha256"]}, sort_keys=True))
    return 0


def _child(args: argparse.Namespace) -> int:
    bundle = read_input_bundle(args.input_root, forbidden_roots=(ROOT,))["bundle"]
    report = run_process(
        bundle,
        producer_commit=args.producer_commit,
        process_index=args.process_index,
    )
    _write_once(args.output_file, report)
    return 0


def _child_command(*, input_root: Path, output_file: Path, producer_commit: str, process_index: int) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "child",
        "--input-root",
        str(input_root),
        "--output-file",
        str(output_file),
        "--producer-commit",
        producer_commit,
        "--process-index",
        str(process_index),
    ]


def _run_child(command: Sequence[str], failure_path: Path) -> None:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(ROOT)
    completed = subprocess.run(command, cwd=ROOT, env=environment, capture_output=True, text=True, check=False)
    if completed.returncode == 0:
        return
    if failure_path.exists():
        failure = _load_object(failure_path)
        body = {key: value for key, value in failure.items() if key != "failure_sha256"}
        if (
            failure.get("schema_version") != "hmm_risk_risk_l1_g2b_failure_v1"
            or failure.get("contract_version") != CONTRACT_VERSION
            or failure.get("failure_sha256") != canonical_sha256(body)
        ):
            raise RiskL1G2BError(REASON_REPRODUCIBILITY, "child failure receipt differs", stage="child_readback")
        raise RiskL1G2BError(
            str(failure.get("reason_code") or REASON_INPUT),
            str(failure.get("message") or "risk child failed"),
            stage=str(failure.get("stage") or "child"),
            evidence=failure.get("evidence") if isinstance(failure.get("evidence"), dict) else {},
        )
    raise RiskL1G2BError(
        REASON_REPRODUCIBILITY,
        f"child exited without durable failure receipt: {completed.stderr[-2000:]}",
        stage="child_readback",
    )


def _run_parent(args: argparse.Namespace) -> int:
    output = _external_new_directory(args.output_root)
    try:
        children = [output / "fresh_process_1.json", output / "fresh_process_2.json"]
        for index, child in enumerate(children, start=1):
            _run_child(
                _child_command(
                    input_root=args.input_root,
                    output_file=child,
                    producer_commit=args.producer_commit,
                    process_index=index,
                ),
                output / f"fresh_process_{index}.failure.json",
            )
        acceptance, model = close_processes(_load_object(children[0]), _load_object(children[1]))
        _write_once(output / "acceptance.json", acceptance)
        _write_once(output / "model.json", model)
        if _load_object(output / "acceptance.json") != acceptance or _load_object(output / "model.json") != model:
            raise RiskL1G2BError(REASON_READBACK, "risk parent readback differs", stage="parent_readback")
    except Exception as exc:
        failure_path = output / "parent.failure.json"
        if not failure_path.exists():
            _write_once(failure_path, _failure(exc, stage="parent"))
        print(json.dumps({"status": "failed", "failure": str(failure_path)}, sort_keys=True), file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": acceptance["status"],
                "output_root": str(output),
                "fit_count": acceptance["fit_count"],
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
    child = subparsers.add_parser("child")
    child.add_argument("--input-root", type=Path, required=True)
    child.add_argument("--output-file", type=Path, required=True)
    child.add_argument("--producer-commit", required=True)
    child.add_argument("--process-index", type=int, choices=(1, 2), required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.mode == "build-input":
            return _build_input(args)
        if args.mode == "child":
            return _child(args)
        return _run_parent(args)
    except Exception as exc:
        if args.mode == "child":
            failure_path = args.output_file.with_name(f"{args.output_file.stem}.failure.json")
            if not failure_path.exists():
                _write_once(failure_path, _failure(exc, stage="child"))
        elif args.mode == "build-input":
            failure_path = args.output_root.parent / f"{args.output_root.name}.failure.json"
            if args.output_root.parent.exists() and not failure_path.exists():
                _write_once(failure_path, _failure(exc, stage="build_input"))
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason_code": str(getattr(exc, "reason_code", REASON_INPUT)),
                    "message": str(exc),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
