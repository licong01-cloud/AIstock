"""Prepare or compare the approved QE HMM three-arm replay.

Preparation is read-only and creates a pending-request JSON.  It never submits
or runs a QE task.  Submission remains an explicit later operation after both
runtime deployments are read back.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.hmm_risk.qe_assistance_three_arm import (  # noqa: E402
    SOURCE_TASK_ID,
    build_three_arm_request,
    compare_three_arm_results,
)
from backend.services.hmm_risk.qe_assistance_transport import (  # noqa: E402
    BINDING_SCHEMA_VERSION,
    formal_runtime_binding,
)
from backend.services.hmm_risk.qe_assistance_adapter import SCHEMA_VERSION  # noqa: E402
from backend.services.qe_templates.validator import (  # noqa: E402
    validate_qe_historical_stock_pool_window,
)


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_new(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        try:
            path.unlink(missing_ok=True)
        finally:
            raise


def _fetch_source_config(base_url: str, loop_index: int) -> dict[str, Any]:
    url = (
        f"{base_url.rstrip('/')}/api/v1/quantevolver/evolution/tasks/"
        f"{SOURCE_TASK_ID}/loops/{loop_index}/config"
    )
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310 - explicit operator URL
        payload = json.load(response)
    data = payload.get("data") if isinstance(payload, dict) else None
    config = data.get("config_json") if isinstance(data, dict) else None
    if isinstance(config, str):
        config = json.loads(config)
    if not isinstance(config, dict):
        raise RuntimeError(f"source Loop{loop_index} config response is invalid")
    return config


def _validate_submission_preflight(api_request: dict[str, Any]) -> None:
    loops = api_request.get("loops")
    if not isinstance(loops, list) or len(loops) != 3:
        raise RuntimeError("three-arm request must contain exactly three loops")
    errors: list[str] = []
    for index, loop in enumerate(loops, start=1):
        if not isinstance(loop, dict):
            errors.append(f"three-arm loop {index} is not an object")
            continue
        errors.extend(
            validate_qe_historical_stock_pool_window(
                loop,
                context=f"hmm_qe_three_arm.loops[{index}]",
            )
        )
    if errors:
        raise RuntimeError("; ".join(errors))


def _prepare(args: argparse.Namespace) -> int:
    artifact = args.artifact.resolve(strict=True)
    binding = formal_runtime_binding(
        {
            "schema_version": BINDING_SCHEMA_VERSION,
            "artifact_schema_version": SCHEMA_VERSION,
            "local_path": str(artifact),
            "remote_path": args.remote_path,
            "file_sha256": args.file_sha256,
            "canonical_sha256": args.canonical_sha256,
            "size_bytes": artifact.stat().st_size,
        }
    )
    request = build_three_arm_request(
        no_hmm_source=_fetch_source_config(args.api_base_url, 1),
        legacy_hmm_source=_fetch_source_config(args.api_base_url, 2),
        artifact_binding=binding,
        task_name=args.task_name,
    )
    _validate_submission_preflight(request["api_request"])
    _write_new(args.output, request)
    print(
        json.dumps(
            {
                "status": "prepared_not_submitted",
                "output": str(args.output),
                "request_sha256": request["request_sha256"],
                "artifact_binding": binding,
                "database_write_performed": False,
                "experiment_started": False,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def _compare(args: argparse.Namespace) -> int:
    payload = _read_json(args.input)
    rows = payload.get("arms") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise RuntimeError("comparison input must contain an arms list")
    result = compare_three_arm_results(rows)
    _write_new(args.output, result)
    print(json.dumps({"status": result["status"], "output": str(args.output)}, ensure_ascii=False, sort_keys=True))
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--artifact", type=Path, required=True)
    prepare.add_argument("--remote-path", required=True)
    prepare.add_argument("--file-sha256", required=True)
    prepare.add_argument("--canonical-sha256", required=True)
    prepare.add_argument("--task-name", required=True)
    prepare.add_argument("--api-base-url", default="http://127.0.0.1:8001")
    prepare.add_argument("--output", type=Path, required=True)
    prepare.set_defaults(handler=_prepare)

    compare = commands.add_parser("compare")
    compare.add_argument("--input", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.set_defaults(handler=_compare)
    return parser


def main() -> int:
    args = _parser().parse_args()
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
