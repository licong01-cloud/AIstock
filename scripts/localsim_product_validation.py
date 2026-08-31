"""Read-only LocalSIM successor product validation.

The script never creates accounts, replays, sessions or scheduler ticks. It
validates the active OpenAPI surface and reads durable account/replay/economic
projections through the successor product API only.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


REQUIRED_PATHS = frozenset(
    {
        "/api/v1/simulation-runtime/localsim/cutover-readiness",
        "/api/v1/simulation-runtime/localsim/accounts",
        "/api/v1/simulation-runtime/localsim/accounts/{account_id}",
        "/api/v1/simulation-runtime/localsim/accounts/{account_id}/runs",
        "/api/v1/simulation-runtime/localsim/accounts/{account_id}/ledger",
        "/api/v1/simulation-runtime/localsim/accounts/{account_id}/performance",
        "/api/v1/simulation-runtime/localsim/replays",
        "/api/v1/simulation-runtime/localsim/replays/{replay_job_id}",
        "/api/v1/simulation-runtime/scheduler/status",
        "/api/v1/simulation-runtime/scheduler/verification-status",
    }
)
FORBIDDEN_PATH_PREFIXES = (
    "/api/v1/paper-v2",
    "/api/v1/simulation-runtime/scheduler/start",
    "/api/v1/simulation-runtime/scheduler/stop",
    "/api/v1/simulation-runtime/scheduler/tick",
)


class LocalSimValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class ApiClient:
    base_url: str
    timeout_seconds: float

    def get(self, path: str) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self.base_url.rstrip('/')}{path}",
            method="GET",
            headers={"Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if not isinstance(payload, dict):
                    raise LocalSimValidationError(f"GET {path} returned a non-object payload")
                return payload
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise LocalSimValidationError(f"GET {path} failed HTTP {exc.code}: {body[:2000]}") from exc
        except urllib.error.URLError as exc:
            raise LocalSimValidationError(f"GET {path} connection failed: {exc.reason}") from exc


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise LocalSimValidationError(message)


def validate(args: argparse.Namespace) -> dict[str, Any]:
    client = ApiClient(args.api_base, args.timeout_seconds)
    openapi_base = args.api_base.rstrip("/").removesuffix("/api/v1")
    openapi = ApiClient(openapi_base, args.timeout_seconds).get("/openapi.json")
    paths = set((openapi.get("paths") or {}).keys())
    _require(REQUIRED_PATHS <= paths, f"successor OpenAPI paths missing: {sorted(REQUIRED_PATHS - paths)}")
    forbidden = sorted(path for path in paths if any(path.startswith(prefix) for prefix in FORBIDDEN_PATH_PREFIXES))
    _require(not forbidden, f"legacy or mutation OpenAPI paths remain: {forbidden}")

    readiness = client.get("/simulation-runtime/localsim/cutover-readiness").get("readiness")
    _require(isinstance(readiness, dict), "cutover readiness payload is missing")
    accounts = client.get("/simulation-runtime/localsim/accounts?limit=200")
    replays = client.get("/simulation-runtime/localsim/replays?limit=200")
    scheduler = client.get("/simulation-runtime/scheduler/status")
    verification = client.get("/simulation-runtime/scheduler/verification-status")
    _require(accounts.get("schema_version") == "localsim_list_response_v1", "account list schema drift")
    _require(replays.get("schema_version") == "localsim_list_response_v1", "replay list schema drift")

    detail: dict[str, Any] | None = None
    if args.account_id:
        detail = {
            "account": client.get(f"/simulation-runtime/localsim/accounts/{args.account_id}"),
            "runs": client.get(f"/simulation-runtime/localsim/accounts/{args.account_id}/runs?limit=200"),
            "ledger": client.get(f"/simulation-runtime/localsim/accounts/{args.account_id}/ledger"),
            "performance": client.get(f"/simulation-runtime/localsim/accounts/{args.account_id}/performance"),
        }
        serialized = json.dumps(detail, ensure_ascii=False, sort_keys=True)
        _require('"portfolio"' not in serialized and '"session"' not in serialized, "successor query leaked legacy DTO")
    replay_detail = (
        client.get(f"/simulation-runtime/localsim/replays/{args.replay_id}") if args.replay_id else None
    )
    return {
        "schema_version": "localsim_product_validation_receipt_v1",
        "ok": True,
        "validated_at": datetime.now(UTC).isoformat(),
        "source_commit": args.expected_source_commit,
        "api_base": args.api_base,
        "read_only": True,
        "required_path_count": len(REQUIRED_PATHS),
        "legacy_path_count": 0,
        "readiness": readiness,
        "account_count": len(accounts.get("items") or []),
        "replay_count": len(replays.get("items") or []),
        "account_detail_checked": detail is not None,
        "replay_detail_checked": replay_detail is not None,
        "scheduler_control_api_enabled": (scheduler.get("scheduler") or {}).get(
            "scheduler_control_api_enabled"
        ),
        "scheduler_verification_present": bool(verification),
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--api-base",
        default=os.environ.get("NEXT_PUBLIC_API_BASE", "http://127.0.0.1:8001/api/v1"),
    )
    result.add_argument("--account-id")
    result.add_argument("--replay-id")
    result.add_argument("--expected-source-commit")
    result.add_argument("--timeout-seconds", type=float, default=30.0)
    return result


def main() -> int:
    try:
        receipt = validate(parser().parse_args())
    except LocalSimValidationError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), flush=True)
        return 1
    print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
