"""Plan/apply an explicit, receipt-bound LocalSIM runtime PIT migration.

Default transport uses the backend API. --in-process permits read-only plans
in a fresh operator process; production application uses the loaded API.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from urllib.parse import quote
from urllib.request import Request, urlopen


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-id", action="append", required=True)
    parser.add_argument("--origin", default="http://127.0.0.1:8001")
    parser.add_argument("--in-process", action="store_true")
    parser.add_argument("--env-file", type=Path, help="credential location for read-only in-process planning")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--plan-receipt", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.apply and args.plan_receipt is None:
        parser.error("--apply requires the exact preceding --plan-receipt")
    if args.apply and args.in_process:
        parser.error("application requires the loaded backend API; --in-process is planning only")
    if args.output.exists():
        parser.error("output already exists; use a new receipt path")
    # This Windows operator must not spill reports into the system drive.
    if sys.platform == "win32" and args.output.resolve().drive.upper() != "X:":
        parser.error("LocalSIM operator receipts must be written on X:")
    accounts = list(dict.fromkeys(args.account_id))
    previous = json.loads(args.plan_receipt.read_text(encoding="utf-8")) if args.apply else None
    if previous is not None and set(previous.get("plans", {})) != set(accounts):
        parser.error("account targets must exactly match the plan receipt")
    service = None
    if args.in_process:
        root = Path(__file__).resolve().parents[3]
        sys.path.insert(0, str(root))
        from dotenv import load_dotenv

        load_dotenv(args.env_file or root / ".env", override=False)
        from backend.services.simulation_runtime.localsim_pit_migration import LocalSimPitMigrationService

        service = LocalSimPitMigrationService()
    output = {"schema_version": "localsim_runtime_pit_operator_receipt_v1",
              "mode": "apply" if args.apply else "plan", "plans": {}, "results": {}}
    blocked = False
    for account in accounts:
        try:
            endpoint = f"{args.origin.rstrip('/')}/api/v1/simulation-runtime/localsim/accounts/{quote(account, safe='')}/pit-migration"
            if args.apply:
                plan = previous["plans"][account]
                output["plans"][account] = plan
                if not plan.get("ready"):
                    output["results"][account] = {"status": "BLOCKED", "blockers": plan.get("blockers")}
                    blocked = True
                    continue
                body = {"plan_sha256": plan["plan_sha256"]}
                request = Request(endpoint, data=json.dumps(body).encode(),
                                  headers={"Content-Type": "application/json"}, method="POST")
                with urlopen(request, timeout=60) as response:
                    result = json.load(response)
                output["results"][account] = result
            else:
                if service is not None:
                    plan = service.plan(account)
                else:
                    with urlopen(endpoint, timeout=60) as response:
                        plan = json.load(response)
                output["plans"][account] = plan
                blocked |= not plan["ready"]
        except Exception as exc:
            output["results"][account] = {"status": "ERROR", "error_type": type(exc).__name__,
                                          "message": str(exc), "context": getattr(exc, "context", None)}
            blocked = True
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(output, stream, ensure_ascii=False, indent=2, default=str)
    print(f"LocalSIM PIT {output['mode']}: {len(accounts)} accounts; "
          f"{'blocked items remain' if blocked else 'completed'}; receipt: {args.output}")
    return 2 if blocked else 0


if __name__ == "__main__":
    raise SystemExit(main())
