"""Schedule reviewed workspace payloads with the normal QE registry and leases.

The caller supplies the reviewed workspace bytes and its result verifier. The
runner controls admission, dependencies and replay of the same submission intent.
It is used by trusted QE orchestration, not an upload API for worker commands.
"""

from __future__ import annotations

import argparse
import asyncio
from base64 import b64decode
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from graphlib import TopologicalSorter
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any

import httpx

from .config_composer import ConfigComposer
from .frozen_inference_contract import canonical_sha
from .qe_absolute_targets import PARAM as ABSOLUTE_TARGET_PARAM, TARGET_FILE
from .qe_active_execution_capacity import (
    PREPARED_ABSOLUTE_TARGET_MODE,
    QEWorkspaceSubmissionCoordinator,
    QEWorkspaceSubmissionPayload,
    QEWorkspaceSubmissionSource,
    QEExecutionSourceClaimFactory,
    prepared_workspace_mode,
    qe_submission_owner_id,
)
from .qe_batch_execution_policy import CPU_NODE, GPU_NODE, loop_compute_kind
from .qe_execution_reservation import PREPARED_WORKSPACE_PARAM
from .qe_run_registry import (
    QE_ARCHIVE_REPLAY_SOURCE_PARAM,
    QE_RUN_REGISTRATION_PARAM,
    QERunRegistry,
    build_qe_run_registration,
    normalize_archive_replay_source,
)
from .qe_workspace_client import QEWorkspaceClient

BuildPayload = Callable[[Mapping[str, Any]], Awaitable[QEWorkspaceSubmissionPayload]]
VerifyResult = Callable[[QEWorkspaceClient, QEWorkspaceSubmissionPayload], Awaitable[Any]]


@dataclass(frozen=True)
class PreparedWorkspaceJob:
    key: str
    experiment_id: str
    node_id: str
    workspace_path: str
    build_payload: BuildPayload
    verify_result: VerifyResult
    prerequisites: tuple[str, ...] = ()


@dataclass(frozen=True)
class PreparedWorkspaceJobResult:
    state: str
    evidence: Any = None
    detail: str | None = None


def prepared_workspace_node(config: Mapping[str, Any]) -> str:
    """Choose the node before composing a node-bound payload."""
    mode = prepared_workspace_mode(config)
    if mode in ("PREDICTION_REPLAY", PREPARED_ABSOLUTE_TARGET_MODE):
        return CPU_NODE
    if mode != "TRAIN_ONLY":
        raise ValueError("QE_PREPARED_WORKSPACE_MODE_MISMATCH")
    info = ConfigComposer()._get_model_info(config.get("model_id"))
    return GPU_NODE if loop_compute_kind(config, info) == "gpu" else CPU_NODE


class QEPreparedWorkspaceBatch:
    def __init__(self) -> None:
        self._coordinator = QEWorkspaceSubmissionCoordinator()
        self._registry = QERunRegistry()
        self._composer = ConfigComposer()

    async def run(
        self,
        batch_sha256: str,
        jobs: Sequence[PreparedWorkspaceJob],
        *,
        dataset_manifest_sha256: str,
        timeout_seconds: float,
    ) -> dict[str, PreparedWorkspaceJobResult]:
        if re.fullmatch(r"[0-9a-f]{64}", dataset_manifest_sha256) is None:
            raise ValueError("QE_PREPARED_BATCH_DATASET_IDENTITY_INVALID")
        by_key = {job.key: job for job in jobs}
        if not jobs or len(by_key) != len(jobs) or len({j.experiment_id for j in jobs}) != len(jobs):
            raise ValueError("QE_PREPARED_BATCH_JOB_IDENTITIES_INVALID")
        if any(dep not in by_key for job in jobs for dep in job.prerequisites):
            raise ValueError("QE_PREPARED_BATCH_DEPENDENCY_MISSING")
        order = tuple(TopologicalSorter({k: j.prerequisites for k, j in by_key.items()}).static_order())
        tasks: dict[str, asyncio.Task[PreparedWorkspaceJobResult]] = {}

        async def run_job(job: PreparedWorkspaceJob) -> PreparedWorkspaceJobResult:
            completed = {}
            for key in job.prerequisites:
                result = await tasks[key]
                if result.state != "verified":
                    return PreparedWorkspaceJobResult("dependency_unavailable", detail=key)
                completed[key] = result.evidence
            try:
                payload = await job.build_payload(completed)
                evidence = await self._execute(batch_sha256, dataset_manifest_sha256, job, payload)
                return PreparedWorkspaceJobResult("verified", evidence=evidence)
            except Exception as exc:
                return PreparedWorkspaceJobResult("requires_reconciliation", detail=f"{type(exc).__name__}: {exc}")

        for key in order:
            tasks[key] = asyncio.create_task(run_job(by_key[key]))
        pending = set(tasks.values())
        try:
            _, pending = await asyncio.wait(pending, timeout=timeout_seconds)
        finally:
            for task in pending:
                # Cancels only the local await, never the worker account or training.
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        return {
            key: (task.result() if task not in pending else PreparedWorkspaceJobResult(
                "observation_timeout", detail="Reconcile the registered intent before retrying."))
            for key, task in tasks.items()
        }

    async def _execute(
        self,
        batch_sha256: str,
        dataset_manifest_sha256: str,
        job: PreparedWorkspaceJob,
        payload: QEWorkspaceSubmissionPayload,
    ) -> Any:
        # Inputs, commands and environment pins stay fixed across resume.
        identity = canonical_sha({
            "batch_sha256": batch_sha256,
            "job": job.key,
            "experiment_id": job.experiment_id,
            "node_id": job.node_id,
            "task_id": payload.task_id,
            "loop_id": payload.loop_id,
            "workspace_path": job.workspace_path,
            "config": dict(payload.config),
            "experiment_files": dict(payload.experiment_files),
            "wsl_command": payload.wsl_command,
            "callback_url": payload.callback_url,
            "postprocess_descriptor": payload.postprocess_descriptor,
            "model_source": payload.model_source,
            "environment": payload.execution_environment_manifest_sha256,
            "environment_snapshot": payload.execution_environment_snapshot_id,
        })
        payload = replace(payload, execution_identity_hash=identity)
        claim, wait = QEExecutionSourceClaimFactory.experiment(
            experiment_id=job.experiment_id, node_id=job.node_id,
            qe_task_id=payload.task_id, qe_loop_id=payload.loop_id,
        )
        source = QEWorkspaceSubmissionSource(
            source_kind="qe_experiment", source_execution_id=job.experiment_id, node_id=job.node_id,
            submission_intent_hash=identity, owner_id=qe_submission_owner_id(),
            claim_source=claim, record_waiting_capacity=wait,
            backtest_only=prepared_workspace_mode(payload.config) in (
                "PREDICTION_REPLAY", PREPARED_ABSOLUTE_TARGET_MODE,
            ),
            prepared_batch_sha256=batch_sha256,
        )
        contract = self._coordinator.prepared_workspace_contract(source, payload)
        if contract["workload"] == PREPARED_ABSOLUTE_TARGET_MODE:
            target = payload.config[ABSOLUTE_TARGET_PARAM]["targets"]
            try:
                raw_targets = b64decode(payload.experiment_files[f"{TARGET_FILE}.b64"], validate=True)
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("QE_PREPARED_ABSOLUTE_TARGET_BYTES_MISMATCH") from exc
            if sha256(raw_targets).hexdigest() != target["sha256"]:
                raise ValueError("QE_PREPARED_ABSOLUTE_TARGET_BYTES_MISMATCH")
            # Verify once before enrolling, outside the capacity retry loop.
            del raw_targets
        async with QEWorkspaceClient.for_node(job.node_id) as client:
            await self._verify_node_binding(client, job.node_id, payload.config, dataset_manifest_sha256)
            params = self._reserve(job, payload, contract)
            payload = replace(payload, config={**dict(payload.config), "model_params": params})
            while True:
                outcome = await self._coordinator.submit(client=client, source=source, payload=payload)
                if not outcome.waiting_capacity:
                    break
                await asyncio.sleep(10)
            if not (outcome.submitted or outcome.remote_acceptance_unknown):
                raise RuntimeError(f"QE prepared workspace submit {outcome.state}: {outcome.detail}")
            while True:
                try:
                    observed = await client.get_loop_status(payload.task_id, payload.loop_id)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code != 404 and exc.response.status_code < 500:
                        raise
                    observed = {}
                except httpx.TransportError:
                    observed = {}
                status = str(observed.get("status", "")).lower()
                if status in {"completed", "succeeded"}:
                    evidence = await job.verify_result(client, payload)
                    if evidence is None:
                        raise RuntimeError("QE_PREPARED_WORKSPACE_RESULT_EVIDENCE_MISSING")
                    return evidence
                if status in {"failed", "error", "cancelled", "canceled"}:
                    reason = observed.get("error_message") or observed.get("reason_code")
                    raise RuntimeError(f"{payload.task_id}/{payload.loop_id} {status}: {reason}")
                await asyncio.sleep(15)

    @staticmethod
    async def _verify_node_binding(
        client: QEWorkspaceClient, node_id: str, config: Mapping[str, Any], manifest: str,
    ) -> None:
        params = config.get("model_params") or {}
        binding = params.get("_qe_direct_v2_dataset_binding") or {}
        summary = params.get("_qe_active_dataset_summary") or {}
        if summary.get("dataset_manifest_sha256") != manifest or not binding.get("candidate_root"):
            raise ValueError("QE_PREPARED_WORKSPACE_DATASET_BINDING_MISMATCH")
        identity = await client.get_dataset_identity(node_id=node_id, data_root_uri=binding["candidate_root"])
        if not identity.complete or (identity.dataset or {}).get("dataset_manifest_sha256") != manifest:
            raise ValueError(f"QE_PREPARED_WORKSPACE_NODE_DATASET_MISMATCH: {node_id}")

    def _reserve(
        self, job: PreparedWorkspaceJob, payload: QEWorkspaceSubmissionPayload, contract: Mapping[str, Any]
    ) -> dict[str, Any]:
        config = payload.config
        params = {**dict(config.get("model_params") or {}), PREPARED_WORKSPACE_PARAM: dict(contract),
                  "execution_node_id": job.node_id}
        mode = contract["workload"]
        if mode == PREPARED_ABSOLUTE_TARGET_MODE:
            params[ABSOLUTE_TARGET_PARAM] = config[ABSOLUTE_TARGET_PARAM]
        factor_names = list(config.get("factor_list") or ())
        split = dict(config.get("data_split") or {})
        params[QE_RUN_REGISTRATION_PARAM] = build_qe_run_registration(
            run_kind="single", source_type="agent", custom_params=params, node_id=job.node_id,
            model_id=config.get("model_id"), factor_names=factor_names, strategy_id=config.get("strategy_id"),
            data_split=split, task_id=payload.task_id, loop_index=payload.loop_index,
        )
        params.pop(QE_ARCHIVE_REPLAY_SOURCE_PARAM, None)
        if mode == "PREDICTION_REPLAY":
            source_ref = normalize_archive_replay_source(
                payload.experiment_files.get("qe_prediction_replay_source_ref.json")
            )
            if source_ref is not None:
                params[QE_ARCHIVE_REPLAY_SOURCE_PARAM] = source_ref
        existing = self._composer._get_experiment_record(job.experiment_id)
        if existing is not None:
            registered = existing.get("custom_params") or {}
            identity_keys = [PREPARED_WORKSPACE_PARAM, QE_RUN_REGISTRATION_PARAM]
            if mode == PREPARED_ABSOLUTE_TARGET_MODE:
                identity_keys.append(ABSOLUTE_TARGET_PARAM)
            if any(registered.get(k) != params[k] for k in identity_keys):
                raise ValueError("QE_PREPARED_WORKSPACE_EXISTING_IDENTITY_CHANGED")
            if QE_ARCHIVE_REPLAY_SOURCE_PARAM not in registered:
                # Resume legacy admissions with their original submitted config.
                params.pop(QE_ARCHIVE_REPLAY_SOURCE_PARAM, None)
            elif registered[QE_ARCHIVE_REPLAY_SOURCE_PARAM] != params.get(QE_ARCHIVE_REPLAY_SOURCE_PARAM):
                raise ValueError("QE_PREPARED_WORKSPACE_EXISTING_IDENTITY_CHANGED")
            return params
        self._registry.reserve_single(
            experiment_id=job.experiment_id, experiment_name=job.key, workspace_path=job.workspace_path,
            factor_names=factor_names, model_id=config.get("model_id"), strategy_id=config.get("strategy_id"),
            data_split=split, custom_params=params,
        )
        return params


def main() -> int:
    """Run a reviewed local plan exporting build_jobs() and DATASET_MANIFEST_SHA256."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--timeout-seconds", required=True, type=float)
    args = parser.parse_args()
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    plan = args.plan.resolve(strict=True)
    plan_bytes = plan.read_bytes()
    batch_sha256 = canonical_sha({"plan": plan_bytes.hex()})
    program: dict[str, Any] = {"__file__": str(plan), "__name__": "qe_prepared_operator_plan"}
    exec(compile(plan_bytes, str(plan), "exec"), program)
    results = asyncio.run(QEPreparedWorkspaceBatch().run(
        batch_sha256, program["build_jobs"](), dataset_manifest_sha256=program["DATASET_MANIFEST_SHA256"],
        timeout_seconds=args.timeout_seconds,
    ))
    print(json.dumps({k: asdict(r) for k, r in results.items()}, ensure_ascii=False, default=str))
    return int(any(r.state != "verified" for r in results.values()))


if __name__ == "__main__":
    raise SystemExit(main())
