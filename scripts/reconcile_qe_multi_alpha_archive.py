"""Preview or restore explicitly named baseline archives; never submit experiments."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def reconcile_archive(
    run_id: str, *, expected_manifest: str, repository: Any, handler: Any,
    status_reader: Any, apply: bool = False,
) -> dict[str, Any]:
    from backend.services.qe_archive.models import sha256_json
    from backend.services.qe_archive.multi_alpha_result_evidence import (
        baseline_only_requested, require_archive_preview,
    )

    source = repository.fetch_multi_alpha_combine_run(run_id)
    if not source or source["run"].get("id") != run_id or not baseline_only_requested(source):
        raise ValueError("archive_reconcile_requires_exact_existing_baseline_run")
    dataset = (source["run"].get("execution_identity_json") or {}).get("dataset") or {}
    if dataset.get("dataset_manifest_sha256") != expected_manifest:
        raise ValueError("archive_reconcile_dataset_identity_mismatch")
    frozen_source_hash = sha256_json(source)
    before = status_reader(run_id)
    outbox = before.get("archive_outbox") or {}
    if outbox and not (
        outbox.get("status") == "completed"
        or (outbox.get("status") == "skipped"
            and outbox.get("error_message") == "qe_lifecycle_no_evaluable_result")
    ):
        raise ValueError("archive_reconcile_outbox_not_terminal_known_skip")
    preview = handler.archive_run(run_id, dry_run=True)
    require_archive_preview(preview, run_id)
    report = {
        "schema_version": "qe_baseline_archive_reconciliation_v1", "run_id": run_id,
        "dataset_manifest_sha256": expected_manifest, "frozen_source_sha256": frozen_source_hash,
        "preview": preview, "outbox_before": outbox, "apply": apply,
        "new_fit_predict_backtest": 0,
    }
    if not apply:
        return report
    written = handler.archive_run(run_id, dry_run=False)
    if written.get("run_id") != run_id or written.get("written") is not True or written.get("skipped_reason"):
        raise ValueError("archive_reconcile_write_not_completed")
    status = status_reader(run_id)
    archived = status.get("archive_run") or {}
    if (status.get("archive_status") != "archived" or archived.get("status") != "succeeded"
            or archived.get("research_valid") is not True):
        raise ValueError("archive_reconcile_readback_invalid")
    # Preserve the historical skip in the receipt. Only close the original
    # delivery event after the real archive has been written and read back.
    if outbox.get("status") == "skipped":
        current = status.get("archive_outbox") or {}
        if (current.get("event_id") != outbox.get("event_id")
                or current.get("status") != "skipped"
                or current.get("error_message") != "qe_lifecycle_no_evaluable_result"):
            raise ValueError("archive_reconcile_outbox_changed")
        repository.complete_skipped_multi_alpha_archive_event(outbox["event_id"], run_id=run_id)
    after = status_reader(run_id)
    if (after.get("archive_status") != "archived"
            or (after.get("archive_outbox") or {}).get("status") != "completed"):
        raise ValueError("archive_reconcile_delivery_incomplete")
    if sha256_json(repository.fetch_multi_alpha_combine_run(run_id)) != frozen_source_hash:
        raise ValueError("archive_reconcile_frozen_source_changed")
    report.update(written=written, readback=after, status="ARCHIVED_VERIFIED_ORIGINAL_RUN_UNCHANGED")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--database", choices=["dev", "production"], default="dev")
    parser.add_argument("--run-id", action="append", required=True)
    parser.add_argument("--expected-manifest", required=True)
    parser.add_argument("--expected-source-commit", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args(argv)
    if len(args.run_id) != len(set(args.run_id)):
        parser.error("duplicate run IDs are not allowed")
    head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
    if head != args.expected_source_commit:
        raise ValueError("archive_reconcile_source_commit_mismatch")
    if args.database == "production" and subprocess.check_output(
        ["git", "-C", str(ROOT), "status", "--porcelain", "--untracked-files=no"], text=True,
    ).strip():
        raise ValueError("archive_reconcile_production_requires_committed_clean_source")
    if args.database == "production" and subprocess.run(
        ["git", "-C", str(ROOT), "merge-base", "--is-ancestor", head, "origin/main"],
        capture_output=True,
    ).returncode != 0:
        raise ValueError("archive_reconcile_production_requires_merged_source")
    from dotenv import dotenv_values

    env = dotenv_values(args.env_file)
    for key, value in env.items():
        if value is not None:
            os.environ[key] = value
    prefix = "TDX_DB_DEV_" if args.database == "dev" else "TDX_DB_"
    for key in ("HOST", "PORT", "NAME", "USER", "PASSWORD"):
        value = env.get(prefix + key)
        if value is None:
            raise ValueError(f"archive_reconcile_target_configuration_missing:{prefix + key}")
        os.environ["TDX_DB_" + key] = value
    if args.database == "dev":
        if ("dev" not in os.environ["TDX_DB_NAME"].lower()
                or tuple(os.environ["TDX_DB_" + k] for k in ("HOST", "PORT", "NAME"))
                == tuple(env.get("TDX_DB_" + k) for k in ("HOST", "PORT", "NAME"))):
            raise ValueError("archive_reconcile_requires_existing_distinct_dev")
    from backend.services.multi_alpha.combine_backtest import MultiAlphaCombineBacktestService
    from backend.services.qe_archive.handlers.multi_alpha_combine_archive_handler import MultiAlphaCombineArchiveHandler
    from backend.services.qe_archive.repository import QEArchiveRepository

    repository = QEArchiveRepository()
    handler = MultiAlphaCombineArchiveHandler(repository=repository)
    service = MultiAlphaCombineBacktestService()
    # Validate every explicit target before making the first archive write.
    previews = [reconcile_archive(rid, expected_manifest=args.expected_manifest, repository=repository,
                                 handler=handler, status_reader=service.get_archive_status) for rid in args.run_id]
    results = ([reconcile_archive(rid, expected_manifest=args.expected_manifest, repository=repository,
                                  handler=handler, status_reader=service.get_archive_status, apply=True)
                for rid in args.run_id] if args.apply else previews)
    receipt = {"source_commit": head, "database": args.database, "env_file": str(args.env_file),
               "apply": args.apply, "results": results, "service_restart": False}
    if args.receipt:
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        with args.receipt.open("x", encoding="utf-8") as stream:
            json.dump(receipt, stream, ensure_ascii=False, indent=2, default=str)
            stream.write("\n")
    print(json.dumps({"source_commit": head, "database": args.database, "apply": args.apply,
                      "run_ids": args.run_id, "receipt": str(args.receipt) if args.receipt else None,
                      "status": "COMPLETE" if args.apply else "PREVIEW_PASS"}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
