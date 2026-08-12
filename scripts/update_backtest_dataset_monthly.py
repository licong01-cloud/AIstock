#!/usr/bin/env python
"""One-command, candidate-only control client for monthly QE dataset releases.

This entrypoint only writes small durable intents/commands or performs bounded
catalog reads.  It never starts a Worker, exporter, backend, database repair,
activation, cleanup, or production data mutation.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
LEGACY_PROFILE_PATH = (REPOSITORY_ROOT / "configs" / "datasets" / "qe_backtest_monthly_v1.yaml").resolve()
CANONICAL_PROFILE_PATH = (REPOSITORY_ROOT / "configs" / "datasets" / "qe_backtest_monthly_v2.yaml").resolve()
PROFILE_PATH = LEGACY_PROFILE_PATH
PROFILE_PATHS = {
    "qe_hmm_full_v1": LEGACY_PROFILE_PATH,
    "qe_hmm_full_v2": CANONICAL_PROFILE_PATH,
}
CLI_PRINCIPAL = "dataset-release-local-cli"

if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.dataset_release.cas_store import CASStoreError  # noqa: E402
from backend.services.dataset_release.control_service import (  # noqa: E402
    DatasetReleaseControlService,
    DatasetReleaseProfileBinding,
)
from backend.services.dataset_release.control_store import (  # noqa: E402
    ControlStoreError,
)
from backend.services.dataset_release.errors import (  # noqa: E402
    DatasetReleaseError,
    ProfileValidationError,
    public_error_envelope,
)
from backend.services.dataset_release.legacy_catalog import (  # noqa: E402
    LegacyCandidateCataloger,
    LegacyCatalogRequest,
)
from backend.services.dataset_release.profile import load_dataset_profile  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Submit or inspect candidate-only monthly QE dataset releases. This command never starts data execution."
        )
    )
    parser.add_argument(
        "--profile",
        choices=tuple(PROFILE_PATHS),
        default="qe_hmm_full_v1",
        help="v1 reproduces existing releases; v2 is the canonical candidate-only monthly profile",
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    monthly = subparsers.add_parser("monthly", help="submit one durable monthly intent")
    monthly.add_argument(
        "--candidate-only",
        action="store_true",
        help="required safety declaration; production activation is excluded",
    )
    monthly.add_argument("--scope", choices=("sample", "full"), default="full")
    monthly.add_argument("--idempotency-key")

    status = subparsers.add_parser("status", help="read bounded durable status")
    status.add_argument("--latest", action="store_true", help="select newest submission")

    events = subparsers.add_parser("events", help="read one bounded event page")
    event_target = events.add_mutually_exclusive_group(required=True)
    event_target.add_argument("--submission-id")
    event_target.add_argument("--run-id")
    events.add_argument("--after-event-id", type=int, default=0)
    events.add_argument("--limit", type=int, default=50)

    receipt = subparsers.add_parser("receipt", help="read one terminal CAS receipt")
    receipt_target = receipt.add_mutually_exclusive_group(required=True)
    receipt_target.add_argument("--run-id")
    receipt_target.add_argument("--submission-id")

    log = subparsers.add_parser("log", help="read one bounded forward log page")
    log.add_argument("--run-id", required=True)
    log.add_argument("--stream", choices=("stdout", "stderr", "worker"), default="stdout")
    log.add_argument("--log-id", type=int, default=0)
    log.add_argument("--generation", type=int, default=1)
    log.add_argument("--byte-offset", type=int, default=0)
    log.add_argument("--max-bytes", type=int, default=256 * 1024)
    log.add_argument("--max-lines", type=int, default=1000)

    reattest = subparsers.add_parser(
        "reattest-existing", help="submit read-only re-attestation of a cataloged candidate"
    )
    reattest.add_argument("--latest", action="store_true", help="select catalog latest only")
    reattest.add_argument("--scope", choices=("sample", "full"), default="full")
    reattest.add_argument("--idempotency-key")

    catalog = subparsers.add_parser(
        "catalog-existing",
        help="hash and catalog one exact allowlisted candidate without modifying it",
    )
    catalog.add_argument("--candidate-path", type=Path, required=True)
    catalog.add_argument("--evidence-manifest", type=Path, required=True)
    catalog.add_argument("--cutoff", type=date.fromisoformat, required=True)
    catalog.add_argument("--scope", choices=("sample", "full"), default="full")

    cancel = subparsers.add_parser("cancel-request", help="enqueue cooperative cancellation")
    target = cancel.add_mutually_exclusive_group(required=True)
    target.add_argument("--submission-id")
    target.add_argument("--run-id")
    cancel.add_argument("--idempotency-key")

    resume = subparsers.add_parser("resume", help="enqueue durable run resume")
    resume.add_argument("--run-id", required=True)
    resume.add_argument("--idempotency-key")
    return parser


def build_control_service() -> DatasetReleaseControlService:
    """Open only the checked-in profile and its explicitly initialized control root."""

    profiles = tuple(load_dataset_profile(path) for path in PROFILE_PATHS.values())
    return DatasetReleaseControlService(tuple(DatasetReleaseProfileBinding.from_profile(item) for item in profiles))


def _selected_profile_id(service: DatasetReleaseControlService, args: argparse.Namespace) -> str:
    requested = str(getattr(args, "profile", "") or "").strip()
    if requested:
        if requested not in service.profile_ids:
            raise ValueError(f"profile is not allowlisted: {requested}")
        return requested
    if len(service.profile_ids) == 1:
        return service.profile_ids[0]
    return "qe_hmm_full_v1"


def _monthly(
    service: DatasetReleaseControlService,
    args: argparse.Namespace,
    *,
    observed_at: datetime,
) -> Mapping[str, Any]:
    if not args.candidate_only:
        raise ValueError("monthly requires --candidate-only")
    profile_id = _selected_profile_id(service, args)
    preview = service.preview_monthly(
        profile_id=profile_id,
        cutoff_policy="auto-previous-month",
        scope=args.scope,
        candidate_only=True,
        now=observed_at,
    )
    idempotency_key = args.idempotency_key or (
        service.monthly_invocation_idempotency_key(
            profile_id=profile_id,
            scope=args.scope,
            logical_request_key=str(preview["logical_request_key"]),
            observed_at=observed_at,
        )
    )
    result = service.submit_monthly(
        profile_id=profile_id,
        cutoff_policy="auto-previous-month",
        scope=args.scope,
        candidate_only=True,
        principal=CLI_PRINCIPAL,
        idempotency_key=idempotency_key,
        route="cli:monthly",
        now=observed_at,
        preview_token=str(preview["preview_token"]),
    )
    return {
        "ok": True,
        "action": "monthly",
        "idempotency_key": idempotency_key,
        **result,
        "execution_started_by_cli": False,
        "production_activation": "not_requested",
    }


def _status(
    service: DatasetReleaseControlService,
    args: argparse.Namespace,
) -> Mapping[str, Any]:
    if not args.latest:
        raise ValueError("status requires --latest")
    status = service.latest_status(_selected_profile_id(service, args))
    submission = status["submission"]
    run = status["run"]
    return {
        "ok": True,
        "action": "status",
        "profile": status["profile"],
        "submission_id": submission["submission_id"],
        "submission_state": submission["state"],
        "run_id": run["run_id"] if run is not None else None,
        "run_state": run["state"] if run is not None else None,
        "outcome": run["outcome"] if run is not None else None,
        "updated_at": (run["updated_at"] if run is not None else submission["updated_at"]),
        "worker_health": status["worker_health"],
        "retention": status["retention"],
        "bounded_read": True,
        "execution_started_by_cli": False,
        "production_activation": status["activation"],
    }


def _events(
    service: DatasetReleaseControlService,
    args: argparse.Namespace,
) -> Mapping[str, Any]:
    if not 1 <= args.limit <= 200 or args.after_event_id < 0:
        raise ValueError("events bounds exceed 200 rows or use a negative cursor")
    rows = service.list_events(
        _selected_profile_id(service, args),
        submission_id=args.submission_id,
        run_id=args.run_id,
        after_event_id=args.after_event_id,
        limit=args.limit + 1,
    )
    items = rows[: args.limit]
    return {
        "ok": True,
        "action": "events",
        "items": items,
        "bounded_read": True,
        "limit": args.limit,
        "has_more": len(rows) > args.limit,
        "next_after_event_id": (items[-1]["event_id"] if items else None),
        "execution_started_by_cli": False,
    }


def _receipt(
    service: DatasetReleaseControlService,
    args: argparse.Namespace,
) -> Mapping[str, Any]:
    profile_id = _selected_profile_id(service, args)
    receipt = (
        service.get_run_receipt(profile_id, args.run_id)
        if args.run_id
        else service.get_submission_receipt(profile_id, args.submission_id)
    )
    return {
        "ok": True,
        "action": "receipt",
        "run_id": args.run_id,
        "submission_id": args.submission_id,
        "receipt": receipt,
        "bounded_read": True,
        "execution_started_by_cli": False,
        "production_activation": "not_requested",
    }


def _log(
    service: DatasetReleaseControlService,
    args: argparse.Namespace,
) -> Mapping[str, Any]:
    if not 1 <= args.max_bytes <= 1024**2 or not 1 <= args.max_lines <= 1000:
        raise ValueError("log read exceeds 1 MiB/1000-line contract")
    page = service.read_run_log(
        _selected_profile_id(service, args),
        args.run_id,
        stream=args.stream,
        log_id=args.log_id,
        generation=args.generation,
        byte_offset=args.byte_offset,
        max_bytes=args.max_bytes,
        max_lines=args.max_lines,
    )
    return {
        "ok": True,
        "action": "log",
        "run_id": args.run_id,
        "data": page,
        "bounded_read": True,
        "execution_started_by_cli": False,
    }


def _reattest(
    service: DatasetReleaseControlService,
    args: argparse.Namespace,
    *,
    observed_at: datetime,
) -> Mapping[str, Any]:
    if not args.latest:
        raise ValueError("reattest-existing requires --latest")
    profile_id = _selected_profile_id(service, args)
    candidate = service.latest_candidate_registration(
        profile_id=profile_id,
        scope=args.scope,
    )
    default_key = service.reattest_invocation_idempotency_key(
        profile_id=profile_id,
        scope=args.scope,
        candidate=candidate,
        observed_at=observed_at,
    )
    result = service.submit_reattest_latest(
        profile_id=profile_id,
        scope=args.scope,
        principal=CLI_PRINCIPAL,
        idempotency_key=args.idempotency_key or default_key,
        route="cli:reattest-existing",
    )
    return {
        "ok": True,
        "action": "reattest-existing",
        **result,
        "candidate_write": "forbidden",
        "execution_started_by_cli": False,
        "production_activation": "not_requested",
    }


def _catalog_existing(
    cataloger: LegacyCandidateCataloger,
    args: argparse.Namespace,
) -> Mapping[str, Any]:
    result = cataloger.catalog(
        LegacyCatalogRequest(
            candidate_path=args.candidate_path,
            evidence_manifest=args.evidence_manifest,
            scope=args.scope,
            cutoff=args.cutoff,
        )
    )
    return {
        "ok": True,
        "action": "catalog-existing",
        "registration_id": result["registration_id"],
        "candidate_identity": result["candidate_identity"],
        "artifact_root": result["artifact_root"],
        "legacy_catalog_receipt_ref": result["legacy_catalog_receipt_ref"],
        "candidate_write": "forbidden",
        "execution_started_by_cli": False,
        "production_activation": "not_requested",
        "source_equivalence": "not_claimed_catalog_only",
    }


def _command(
    service: DatasetReleaseControlService,
    args: argparse.Namespace,
) -> Mapping[str, Any]:
    profile_id = _selected_profile_id(service, args)
    if args.action == "resume":
        target_type = "run"
        target_id = args.run_id
        command_type = "RESUME_REQUESTED"
        route = "cli:resume"
    else:
        target_type = "submission" if args.submission_id else "run"
        target_id = args.submission_id or args.run_id
        command_type = "CANCEL_REQUESTED"
        route = "cli:cancel-request"
    idempotency_key = args.idempotency_key or f"dsc_{uuid.uuid4().hex}"
    command = service.enqueue_command(
        profile_id=profile_id,
        target_type=target_type,
        target_id=target_id,
        command_type=command_type,
        principal=CLI_PRINCIPAL,
        route=route,
        idempotency_key=idempotency_key,
    )
    return {
        "ok": True,
        "action": args.action,
        "command_id": command["command_id"],
        "command_state": command["state"],
        "target_type": target_type,
        "target_id": target_id,
        "idempotency_key": idempotency_key,
        "replayed": bool(command["replayed"]),
        "process_control": "not_requested",
        "execution_started_by_cli": False,
    }


def main(
    argv: Sequence[str] | None = None,
    *,
    service: DatasetReleaseControlService | None = None,
    cataloger: LegacyCandidateCataloger | None = None,
    observed_at: datetime | None = None,
) -> int:
    args = _parser().parse_args(argv)
    try:
        control = service or build_control_service()
        _selected_profile_id(control, args)
        if args.action == "monthly":
            result = _monthly(
                control,
                args,
                observed_at=observed_at or datetime.now(UTC),
            )
        elif args.action == "status":
            result = _status(control, args)
        elif args.action == "events":
            result = _events(control, args)
        elif args.action == "receipt":
            result = _receipt(control, args)
        elif args.action == "log":
            result = _log(control, args)
        elif args.action == "reattest-existing":
            result = _reattest(
                control,
                args,
                observed_at=observed_at or datetime.now(UTC),
            )
        elif args.action == "catalog-existing":
            selected_cataloger = cataloger
            if selected_cataloger is None:
                profile = load_dataset_profile(PROFILE_PATHS[_selected_profile_id(control, args)])
                selected_cataloger = LegacyCandidateCataloger(
                    service=control,
                    profile=profile,
                )
            result = _catalog_existing(selected_cataloger, args)
        else:
            result = _command(control, args)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except (
        CASStoreError,
        ControlStoreError,
        DatasetReleaseError,
        ProfileValidationError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    **public_error_envelope(
                        exc,
                        fallback_code="DATASET_RELEASE_MONTHLY_CLI_FAILED",
                    ),
                    "execution_started_by_cli": False,
                    "production_activation": "not_requested",
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
