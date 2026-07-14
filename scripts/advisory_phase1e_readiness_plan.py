"""Standalone CLI for Phase 1E historical-research readiness plans."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg2

if __package__ in {None, ""}:  # Support direct execution from the repository root.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.services.advisory_phase0a.evidence_projection_postgres import AdvisoryPostgresEvidenceProjection
from backend.services.advisory_phase0a.policy import POLICY_REGISTRY_ROOT, load_frozen_policy_registry
from backend.services.advisory_phase1.readiness_plan import (
    Phase1EError,
    Phase1ERevalidationBatchRequest,
    Phase1EReadinessPlanCompiler,
    RegistrySourceRequirementCompiler,
    SourceRequirementRegistry,
)
from backend.services.advisory_phase1.readiness_plan_postgres import PostgresPhase1EInputProvider
from backend.services.advisory_phase1.readiness_plan_store import ContentAddressedPlanStore, Phase1EArtifactStoreError
from backend.services.advisory_phase1.source_capacity import CapacityPlanningReceipt, CapacityPlanningRequest


LOGGER = logging.getLogger("aistock.advisory.phase1e")


class Phase1ECommandError(RuntimeError):
    pass


def _load_env_file(path: Path | None) -> None:
    if path is None or not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _db_config(*, target_db: str) -> dict[str, Any]:
    prefix = "TDX_DB_DEV" if target_db == "dev" else "TDX_DB"
    required = [f"{prefix}_{name}" for name in ("HOST", "PORT", "NAME", "USER", "PASSWORD")]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise Phase1ECommandError(f"database connection keys are missing from the configured env file: {missing}")
    return {
        "host": os.environ[f"{prefix}_HOST"],
        "port": int(os.environ[f"{prefix}_PORT"]),
        "dbname": os.environ[f"{prefix}_NAME"],
        "user": os.environ[f"{prefix}_USER"],
        "password": os.environ[f"{prefix}_PASSWORD"],
    }


@contextmanager
def _readonly_connection(*, env_file: Path | None, target_db: str) -> Iterator[Any]:
    _load_env_file(env_file)
    connection = psycopg2.connect(**_db_config(target_db=target_db))
    connection.set_session(readonly=True, autocommit=False, isolation_level="REPEATABLE READ")
    try:
        yield connection
    finally:
        connection.close()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Phase1ECommandError(f"cannot read JSON input {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Phase1ECommandError(f"JSON input {path} must be an object")
    return value


def _structured_error(*, stage: str, error: Exception) -> dict[str, Any]:
    if isinstance(error, Phase1EError):
        return {
            "ok": False,
            "stage": stage,
            "reason_code": error.reason_code,
            "message": str(error),
            "context": error.context,
        }
    if isinstance(error, Phase1EArtifactStoreError):
        return {
            "ok": False,
            "stage": stage,
            "reason_code": error.reason_code,
            "message": str(error),
            "context": error.context,
        }
    return {
        "ok": False,
        "stage": stage,
        "reason_code": "ADVISORY_PHASE1E_UNEXPECTED_ERROR",
        "message": f"{type(error).__name__}: {error}",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compile or inspect Phase 1E historical-research readiness plans.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    compile_batch = subparsers.add_parser("compile-batch")
    compile_batch.add_argument("--request", required=True, type=Path)
    compile_batch.add_argument("--source-requirement-registry", required=True, type=Path)
    compile_batch.add_argument("--capacity-request", required=True, type=Path)
    compile_batch.add_argument("--capacity-receipt", required=True, type=Path)
    compile_batch.add_argument("--policy-registry-id", required=True)
    compile_batch.add_argument("--policy-version", required=True)
    compile_batch.add_argument("--policy-registry-root", type=Path, default=POLICY_REGISTRY_ROOT)
    compile_batch.add_argument("--env-file", type=Path, default=Path(os.environ.get("AISTOCK_ENV_FILE", ".env")))
    compile_batch.add_argument("--target-db", choices=("dev", "prod"), default="dev")
    for command in ("verify-plan", "inspect-plan"):
        action = subparsers.add_parser(command)
        action.add_argument("--kind", choices=("audit", "plan", "batch"), required=True)
        action.add_argument("--identity", required=True)
        action.add_argument("--semantic-hash", required=True)
        action.add_argument("--artifact-store-policy-hash", required=True)
    return parser


def _compile(args: argparse.Namespace) -> dict[str, Any]:
    batch_request = Phase1ERevalidationBatchRequest.model_validate(_read_json(args.request))
    registry = SourceRequirementRegistry.model_validate(_read_json(args.source_requirement_registry))
    capacity_request = CapacityPlanningRequest.model_validate(_read_json(args.capacity_request))
    capacity_receipt = CapacityPlanningReceipt.model_validate(_read_json(args.capacity_receipt))
    policy = load_frozen_policy_registry(
        policy_registry_id=args.policy_registry_id,
        policy_version=args.policy_version,
        registry_root=args.policy_registry_root,
    )
    if policy.registry_content_hash != batch_request.phase0a_policy_hash:
        raise Phase1ECommandError("frozen policy registry hash does not match Phase 1E batch request")
    if capacity_request.request_hash != capacity_receipt.request_hash:
        raise Phase1ECommandError("capacity receipt request hash does not match capacity request")
    store = ContentAddressedPlanStore.from_environment(policy_hash=batch_request.artifact_store_policy_hash)

    def factory() -> Iterator[Any]:
        return _readonly_connection(env_file=args.env_file, target_db=args.target_db)

    provider = PostgresPhase1EInputProvider(
        projection=AdvisoryPostgresEvidenceProjection(factory),
        policy=policy,
    )
    compiler = Phase1EReadinessPlanCompiler(
        source_requirement_compiler=RegistrySourceRequirementCompiler(registry),
        capacity_request=capacity_request,
        capacity_receipt=capacity_receipt,
        artifact_store=store,
    )
    plans, receipt = compiler.compile_batch(request=batch_request, provider=provider)
    complete = not receipt.failed_input_scopes
    return {
        "ok": complete,
        "status": "complete" if complete else "partial",
        "batch_receipt_hash": receipt.batch_receipt_hash,
        "plan_count": len(plans),
        "plan_hashes": [plan.plan_hash for plan in plans],
        "failed_input_scope_count": len(receipt.failed_input_scopes),
        "failed_input_scopes": [item.model_dump(mode="json") for item in receipt.failed_input_scopes],
        "research_only": True,
        "execution_prohibited": True,
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "compile-batch":
            result = _compile(args)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
            return 0 if result["ok"] else 3
        store = ContentAddressedPlanStore.from_environment(policy_hash=args.artifact_store_policy_hash)
        document = store.verify(kind=args.kind, identity=args.identity, semantic_hash=args.semantic_hash)
        if args.command == "verify-plan":
            print(json.dumps({"ok": True, "kind": args.kind, "identity": args.identity, "semantic_hash": args.semantic_hash}, sort_keys=True))
        else:
            print(json.dumps(document, ensure_ascii=False, sort_keys=True))
        return 0
    except (Phase1ECommandError, Phase1EError, Phase1EArtifactStoreError, ValueError) as exc:
        print(json.dumps(_structured_error(stage=args.command, error=exc), ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("advisory_phase1e_unexpected_error command=%s", args.command)
        print(json.dumps(_structured_error(stage=args.command, error=exc), ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
