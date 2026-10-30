"""Recover existing Advisory publications; no clock override or service control."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through", required=True, type=date.fromisoformat)
    parser.add_argument("--program-id", action="append", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-commit")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    args = parser.parse_args()
    if args.through >= datetime.now(ZoneInfo("Asia/Shanghai")).date():
        parser.error("--through must be a past decision date")
    if len(set(args.program_id)) != len(args.program_id):
        parser.error("program identities must be distinct")
    if args.apply:
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
        if not args.expected_commit or head != args.expected_commit or dirty:
            parser.error("apply requires a clean checkout at --expected-commit")
        subprocess.run(["git", "merge-base", "--is-ancestor", head, "origin/main"], cwd=ROOT, check=True)
    from dotenv import load_dotenv
    if not args.env_file.is_file():
        parser.error("credential configuration file does not exist; supply --env-file")
    load_dotenv(args.env_file, override=False)
    from backend.services.advisory_forward.service import AdvisoryForwardService
    service = AdvisoryForwardService()
    pending = [r for r in service.repository.pending_publications(
        before=datetime.now(ZoneInfo("Asia/Shanghai")).date(), program_ids=args.program_id,
    ) if r["decision_as_of_trade_date"] <= args.through]
    if not args.apply:
        rows = []
        for r in pending:
            if r["newer_published"]:
                rows.append({k: r[k] for k in ("forward_run_id", "program_id", "decision_as_of_trade_date")}
                            | {"status": "SKIPPED_SUPERSEDED_BY_PUBLISHED"})
                continue
            binding = service.program_service.active_binding(r["program_id"])
            checks = []
            for package in binding["package_ids"]:
                readiness = service.input_readiness.service.readiness(
                    data_date=r["decision_as_of_trade_date"].isoformat(), consumer="ADVISORY", binding_id=package,
                )
                checks.append({k: readiness.get(k) for k in ("binding_id", "state", "prediction_ready", "reason_code")}
                              | {"blocker_count": len(readiness.get("blockers") or [])})
            rows.append({k: r[k] for k in ("forward_run_id", "program_id", "decision_as_of_trade_date", "target_trade_date")}
                        | {"newer_published": r["newer_published"], "readiness": checks})
        result = {"mode": "read_only", "pending": rows}
    else:
        result = service.recover_pending_publications(through=args.through, program_ids=args.program_id)
        result["mode"] = "apply"
        result["readback"] = [
            {k: r.get(k) for k in ("forward_run_id", "program_id", "decision_as_of_trade_date", "target_trade_date",
                                   "publication_status", "settlement_status", "published_at", "list_version_id", "last_reason_code")}
            for pid in args.program_id for r in service.repository.list_runs(program_id=pid, limit=500)
            if r["forward_run_id"] in {p["forward_run_id"] for p in pending}
        ]
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 1 if result.get("status") in {"BLOCKED", "ALREADY_RUNNING"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
