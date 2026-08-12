#!/usr/bin/env python
"""Independent dataset-release Worker CLI with strict production wiring."""

from __future__ import annotations

import argparse
import json
import re
import signal
import subprocess
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path, PureWindowsPath
from typing import Mapping, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ALLOWLISTED_PROFILE = (REPOSITORY_ROOT / "configs" / "datasets" / "qe_backtest_monthly_v1.yaml").resolve()
WORKER_DEPENDENCY_PATHS = (
    "backend/services/dataset_release",
    "scripts/dataset_release_worker.py",
    "scripts/dataset_release_build_stage.py",
    "scripts/dataset_release_candidate_consumer_smoke.py",
    "scripts/dataset_release_source_recheck.py",
    "scripts/dataset_release_source_stage.py",
    "configs/datasets/qe_backtest_monthly_v1.yaml",
)
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.dataset_release.control_store import (  # noqa: E402
    ControlStore,
    ControlStoreError,
)
from backend.services.dataset_release.cas_store import CASStore  # noqa: E402
from backend.services.dataset_release.build_processor import (  # noqa: E402
    ProductionBuildProcessor,
)
from backend.services.dataset_release.errors import (  # noqa: E402
    DatasetReleaseError,
    public_error_envelope,
)
from backend.services.dataset_release.profile import (  # noqa: E402
    DatasetProfile,
    ProfileValidationError,
    load_dataset_profile,
)
from backend.services.dataset_release.control_service import (  # noqa: E402
    resolve_previous_month_trading_cutoff,
)
from backend.services.dataset_release.process_liveness import (  # noqa: E402
    LocalProcessTreeLivenessProbe,
)
from backend.services.dataset_release.publisher import DatasetPublisher  # noqa: E402
from backend.services.dataset_release.resolution_processor import (  # noqa: E402
    build_resolution_processor,
)
from backend.services.dataset_release.runtime_adapters import (  # noqa: E402
    DurableWslQuiescenceReader,
    FencedPublishRecoveryAdapter,
    WslSystemdQuiescenceProbe,
)
from backend.services.dataset_release.reconciler import (  # noqa: E402
    MonthlyDatasetReconciler,
    ReconcileError,
    default_reconcile_owner_identity,
)
from backend.services.dataset_release.resource_budget import (  # noqa: E402
    HostTelemetrySampler,
    ResourceTelemetryUnavailable,
)
from backend.services.dataset_release.resource_gate import ResourceGate  # noqa: E402
from backend.services.dataset_release.worker import (  # noqa: E402
    DEFAULT_POLL_SECONDS,
    DatasetReleaseWorker,
    MAX_DRAIN_JOBS,
    ProcessorRegistry,
    ProcessorUnavailable,
    WorkResourceSpec,
    WorkerError,
)
from backend.services.dataset_release.worker_identity import (  # noqa: E402
    WorkerHeartbeatStore,
    WorkerIdentity,
    WorkerIdentityError,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the independent candidate-only dataset-release Worker.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="claim at most one item")
    mode.add_argument("--drain", action="store_true", help="process a bounded item count")
    mode.add_argument("--serve", action="store_true", help="poll until cooperative shutdown")
    mode.add_argument(
        "--preflight",
        action="store_true",
        help="read-only production wiring/toolchain/code readiness check",
    )
    mode.add_argument(
        "--reconcile",
        action="store_true",
        help="run one disabled-by-default, control-only monthly catch-up cycle",
    )
    parser.add_argument("--max-jobs", type=int, help="required positive bound for --drain")
    parser.add_argument("--control-root", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    return parser


def build_default_registry(
    profile: DatasetProfile,
    *,
    store: ControlStore,
) -> ProcessorRegistry:
    """Construct every production processor from one profile/control authority."""

    if not _same_windows_path(profile.control_root, store.root):
        raise WorkerError(
            "registry control root differs from the allowlisted profile",
            code="BLOCKED_CONTROL_ROOT_NOT_ALLOWLISTED",
        )
    candidate_root = Path(str(profile.candidate_root)).resolve(strict=True)
    cas = CASStore(store.root)
    qlib_toolchain = profile.qlib_toolchain.build_verified(REPOSITORY_ROOT)
    publisher = DatasetPublisher(store, candidate_root=candidate_root)
    return ProcessorRegistry(
        resolution=build_resolution_processor(profile, store, cas),
        build=ProductionBuildProcessor(
            profile=profile,
            profile_path=ALLOWLISTED_PROFILE,
            project_root=REPOSITORY_ROOT,
            store=store,
            cas=cas,
            candidate_root=candidate_root,
            qlib_toolchain=qlib_toolchain,
        ),
        dependency_paths=WORKER_DEPENDENCY_PATHS,
        publish_recovery=FencedPublishRecoveryAdapter(store, publisher),
        wsl_quiescence=DurableWslQuiescenceReader(
            store,
            active_ttl_seconds=max(3.0, profile.resource_policy.enforcement_sample_seconds * 5),
            recovery_probe=WslSystemdQuiescenceProbe(
                expected_distro=qlib_toolchain.distro,
                guardian_python=qlib_toolchain.guardian_python,
                guardian_script_wsl=qlib_toolchain.guardian_script_wsl,
            ),
        ),
    )


def build_resource_gate_factory(
    profile: DatasetProfile,
    host_sampler: HostTelemetrySampler,
):
    """Bind every production attempt to the exact allowlisted profile policy."""

    def factory(resources: WorkResourceSpec, _stage: str) -> ResourceGate:
        if resources.policy != profile.resource_policy:
            raise WorkerError(
                "processor resource policy differs from the allowlisted profile",
                code="BLOCKED_RESOURCE_POLICY_DRIFT",
            )
        return ResourceGate(
            profile,
            host_probe=host_sampler,
            predicted_new_bytes=resources.predicted_new_bytes,
        )

    return factory


def _same_windows_path(configured: PureWindowsPath, supplied: Path) -> bool:
    return (
        str(configured).replace("/", "\\").casefold()
        == str(supplied.resolve(strict=True)).replace("/", "\\").casefold()
    )


def _read_repo_code_sha(
    dependency_paths: Sequence[str],
    *,
    runner=None,
) -> str:
    run = runner or subprocess.run
    head = run(
        ["git", "-C", str(REPOSITORY_ROOT), "rev-parse", "--verify", "HEAD"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
        check=False,
    )
    code_sha = str(head.stdout).strip().lower() if head.returncode == 0 else ""
    if re.fullmatch(r"[0-9a-f]{40,64}", code_sha) is None:
        raise WorkerError(
            "deployed git SHA cannot be read",
            code="BLOCKED_WORKER_CODE_SHA_UNAVAILABLE",
        )
    paths = tuple(dict.fromkeys((*WORKER_DEPENDENCY_PATHS, *dependency_paths)))
    missing = [value for value in paths if not (REPOSITORY_ROOT / value).exists()]
    if missing:
        raise WorkerError(
            "registered Worker dependency path is missing",
            code="BLOCKED_WORKER_DEPENDENCY_MISSING",
            context={"missing_path_count": len(missing)},
        )
    dirty = run(
        [
            "git",
            "-C",
            str(REPOSITORY_ROOT),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--",
            *paths,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
        check=False,
    )
    if dirty.returncode != 0:
        raise WorkerError(
            "deployed dependency dirty-state cannot be read",
            code="BLOCKED_WORKER_DIRTY_STATE_UNAVAILABLE",
        )
    dirty_lines = [line for line in str(dirty.stdout).splitlines() if line.strip()]
    if dirty_lines:
        raise WorkerError(
            "deployed Worker/processor dependency paths are dirty",
            code="BLOCKED_WORKER_CODE_DIRTY",
            context={"dirty_path_count": len(dirty_lines)},
        )
    return code_sha


def _run_preflight(
    *,
    profile: DatasetProfile,
    store: ControlStore,
) -> Mapping[str, object]:
    """Validate production wiring without claim, heartbeat, or Worker startup."""

    processors = build_default_registry(profile, store=store)
    processors.assert_production_ready()
    code_sha = _read_repo_code_sha(processors.dependency_paths)
    build = processors.build
    toolchain = getattr(build, "qlib_toolchain_receipt", None)
    if not isinstance(toolchain, Mapping) or not isinstance(toolchain.get("files"), Mapping):
        raise WorkerError(
            "build processor omitted verified Qlib toolchain evidence",
            code="BLOCKED_WORKER_TOOLCHAIN_EVIDENCE_MISSING",
        )
    files = {
        str(name): {
            "sha256": str(value.get("sha256", "")),
            "size_bytes": int(value.get("size_bytes", -1)),
        }
        for name, value in sorted(toolchain["files"].items())
        if isinstance(value, Mapping)
    }
    if len(files) != len(toolchain["files"]):
        raise WorkerError(
            "Qlib toolchain file evidence is invalid",
            code="BLOCKED_WORKER_TOOLCHAIN_EVIDENCE_MISSING",
        )
    return {
        "schema_version": "dataset_release_worker_preflight_v1",
        "ok": True,
        "mode": "preflight",
        "profile": profile.profile,
        "profile_config_digest": profile.config_digest,
        "semantic_profile_digest": profile.semantic_profile_digest,
        "control_store_id": str(store.identity["control_store_id"]),
        "control_schema_version": int(store.identity["control_schema_version"]),
        "registry_gate": "ready",
        "registered_processors": [
            "resolution",
            "build",
            "publish_recovery",
            "wsl_quiescence",
        ],
        "dependency_path_count": len(processors.dependency_paths),
        "code_sha": code_sha,
        "qlib_toolchain_digest": str(toolchain.get("toolchain_digest", "")),
        "qlib_toolchain_files": files,
        "safety": {
            "claims": 0,
            "heartbeat_writes": 0,
            "worker_started": False,
            "data_process_started": False,
            "database_writes": 0,
            "candidate_writes": 0,
            "production_writes": 0,
            "service_process_controls": 0,
        },
    }


def main(
    argv: Sequence[str] | None = None,
    *,
    registry: ProcessorRegistry | None = None,
) -> int:
    args = _parser().parse_args(argv)
    if args.drain and (args.max_jobs is None or not 0 < args.max_jobs <= MAX_DRAIN_JOBS):
        _parser().error(f"--drain requires --max-jobs N with 1 <= N <= {MAX_DRAIN_JOBS}")
    if not args.drain and args.max_jobs is not None:
        _parser().error("--max-jobs is valid only with --drain")
    try:
        supplied_profile = args.profile.expanduser().resolve(strict=True)
        if supplied_profile != ALLOWLISTED_PROFILE:
            raise WorkerError(
                "profile path is not in the Worker allowlist",
                code="BLOCKED_PROFILE_NOT_ALLOWLISTED",
            )
        profile = load_dataset_profile(supplied_profile)
        store = ControlStore(args.control_root, read_only=args.preflight)
        if not _same_windows_path(profile.control_root, store.root):
            raise WorkerError(
                "explicit control root differs from the allowlisted profile root",
                code="BLOCKED_CONTROL_ROOT_NOT_ALLOWLISTED",
            )
        if args.reconcile:
            report = MonthlyDatasetReconciler(
                profile=profile,
                store=store,
                cutoff_resolver=resolve_previous_month_trading_cutoff,
                enabled=True,
            ).run_once(owner_identity=default_reconcile_owner_identity())
            print(
                json.dumps(
                    {
                        "ok": report.state not in {"PARTIAL_FAILURE"},
                        "mode": "reconcile",
                        "state": report.state,
                        "profile": report.profile,
                        "cycle_id": report.cycle_id,
                        "fence": report.fence,
                        "items": [
                            {
                                "cutoff": item.cutoff,
                                "logical_request_key": item.logical_request_key,
                                "disposition": item.disposition,
                                "submission_id": item.submission_id,
                                "detail": item.detail,
                            }
                            for item in report.items
                        ],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 2 if report.state == "PARTIAL_FAILURE" else 0
        if args.preflight:
            print(
                json.dumps(
                    _run_preflight(profile=profile, store=store),
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        base_code_sha = _read_repo_code_sha(())
        processors = registry or build_default_registry(profile, store=store)
        try:
            processors.assert_production_ready()
        except ProcessorUnavailable:
            blocked_identity = WorkerIdentity.create(
                code_sha=base_code_sha,
                profile_digests={profile.profile: profile.config_digest},
                capabilities=("processor-registration-blocked",),
            )
            WorkerHeartbeatStore(store).write(
                blocked_identity,
                status="BLOCKED_PROCESSOR_UNAVAILABLE",
                observed_at=datetime.now(UTC),
            )
            raise
        code_sha = _read_repo_code_sha(processors.dependency_paths)
        if code_sha != base_code_sha:
            raise WorkerError(
                "repository HEAD changed during Worker startup",
                code="BLOCKED_WORKER_CODE_SHA_DRIFT",
            )
        identity = WorkerIdentity.create(
            code_sha=code_sha,
            profile_digests={profile.profile: profile.config_digest},
            capabilities=(
                "resolution",
                "build",
                "commands",
                "orphan-reconcile",
                "publish-recovery",
            ),
        )
        stop_event = threading.Event()

        def request_stop(_signum, _frame) -> None:
            stop_event.set()

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)
        host_sampler = HostTelemetrySampler()
        worker: DatasetReleaseWorker | None = None
        try:
            worker = DatasetReleaseWorker(
                store=store,
                identity=identity,
                registry=processors,
                resource_gate_factory=build_resource_gate_factory(profile, host_sampler),
                liveness_probe=LocalProcessTreeLivenessProbe(
                    identity_reader=WorkerHeartbeatStore(store).read,
                    wsl_quiescence_reader=processors.wsl_quiescence,
                ),
                poll_seconds=DEFAULT_POLL_SECONDS,
                stop_event=stop_event,
            )
            if args.once:
                reports = (worker.run_once(),)
            elif args.drain:
                reports = worker.run_drain(max_jobs=args.max_jobs)
            else:
                reports = worker.run_serve()
        finally:
            if worker is not None:
                worker.close()
            host_sampler.close()
        print(
            json.dumps(
                {
                    "ok": True,
                    "mode": "once" if args.once else "drain" if args.drain else "serve",
                    "claimed": sum(1 for item in reports if item.claimed),
                    "last_state": reports[-1].state if reports else "IDLE",
                    "worker_instance_id": identity.instance_id,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    except (
        ControlStoreError,
        DatasetReleaseError,
        ProfileValidationError,
        ProcessorUnavailable,
        ReconcileError,
        ResourceTelemetryUnavailable,
        WorkerError,
        WorkerIdentityError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    **public_error_envelope(
                        exc,
                        fallback_code="DATASET_RELEASE_WORKER_STARTUP_FAILED",
                    ),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
