from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "aistock_validation_center_readonly_smoke_v1"
DEFAULT_OUTPUT = ROOT / "tmp" / "validation" / "validation_center" / "readonly_smoke.json"


class SmokeConfigError(ValueError):
    """Raised when the smoke run is configured to touch a forbidden endpoint."""


def _now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _repo_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return str(resolved)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _default_api_base() -> str:
    raw = os.environ.get("VALIDATION_CENTER_API_BASE") or os.environ.get("NEXT_PUBLIC_API_BASE")
    if raw:
        return raw
    backend_port = os.environ.get("BACKEND_PORT", "8011")
    return f"http://127.0.0.1:{backend_port}/api/v1"


def _normalize_api_base(raw: str) -> str:
    text = raw.strip().rstrip("/")
    if not text:
        raise SmokeConfigError("api base must not be empty")
    return text


def _validate_api_base(api_base: str, *, allow_production_8001: bool, allow_non_localhost: bool) -> None:
    parsed = urllib.parse.urlparse(api_base)
    if parsed.scheme not in {"http", "https"}:
        raise SmokeConfigError(f"api base must use http/https: {api_base}")
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"} and not allow_non_localhost:
        raise SmokeConfigError("refusing to touch non-localhost validation API")
    if parsed.port == 8001 and not allow_production_8001:
        raise SmokeConfigError("refusing to touch production backend port 8001")


def _is_production_8001(api_base: str) -> bool:
    return urllib.parse.urlparse(api_base).port == 8001


def _url(api_base: str, path: str) -> str:
    return f"{api_base.rstrip('/')}/{path.lstrip('/')}"


def _request_json(api_base: str, path: str, *, timeout: float) -> tuple[int | None, dict[str, Any] | None, str | None]:
    request = urllib.request.Request(
        _url(api_base, path),
        method="GET",
        headers={"Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            text = response.read().decode("utf-8")
            payload = json.loads(text) if text.strip() else {}
            if not isinstance(payload, dict):
                return response.status, None, "JSON root is not an object"
            return response.status, payload, None
    except urllib.error.HTTPError as exc:
        return exc.code, None, f"HTTP {exc.code}: {exc.reason}"
    except urllib.error.URLError as exc:
        return None, None, f"connection failed: {exc.reason}"
    except TimeoutError:
        return None, None, "timeout"
    except json.JSONDecodeError as exc:
        return None, None, f"invalid JSON: {exc}"
    except UnicodeDecodeError as exc:
        return None, None, f"invalid UTF-8: {exc}"


def _is_object(value: Any) -> bool:
    return isinstance(value, dict)


def _is_page(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and isinstance(value.get("items"), list)
        and isinstance(value.get("total"), int)
        and isinstance(value.get("page"), int)
        and isinstance(value.get("page_size"), int)
        and isinstance(value.get("has_more"), bool)
    )


def _extract_data(result: dict[str, Any], failures: list[str]) -> dict[str, Any] | None:
    if not result["ok"]:
        failures.append(f"{result['path']} request failed: {result.get('error') or result.get('status_code')}")
        return None
    payload = result.get("payload")
    if not isinstance(payload, dict):
        failures.append(f"{result['path']} returned non-object payload")
        return None
    if payload.get("status") != "success":
        failures.append(f"{result['path']} envelope status is not success: {payload.get('status')}")
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        failures.append(f"{result['path']} envelope data is not an object")
        return None
    return data


def _call(api_base: str, path: str, *, timeout: float) -> dict[str, Any]:
    status_code, payload, error = _request_json(api_base, path, timeout=timeout)
    ok = error is None and status_code is not None and 200 <= status_code < 300
    return {
        "method": "GET",
        "path": path,
        "status_code": status_code,
        "ok": ok,
        "error": error,
        "payload": payload,
    }


def _append_endpoint(endpoints: list[dict[str, Any]], result: dict[str, Any]) -> dict[str, Any]:
    stored = dict(result)
    payload = stored.pop("payload", None)
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict):
        stored["data_keys"] = sorted(str(key) for key in data.keys())
    endpoints.append(stored)
    return result


def _first_id(page: dict[str, Any], key: str) -> str | None:
    items = page.get("items")
    if not isinstance(items, list) or not items:
        return None
    value = items[0].get(key) if isinstance(items[0], dict) else None
    return str(value) if value else None


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def run_smoke(
    *,
    api_base: str,
    output: Path,
    timeout: float = 5.0,
    page_size: int = 5,
    allow_production_8001: bool = False,
    allow_non_localhost: bool = False,
) -> int:
    started_at = _now_iso()
    api_base = _normalize_api_base(api_base)
    endpoints: list[dict[str, Any]] = []
    failures: list[str] = []
    counts: dict[str, Any] = {}
    production_8001_touched = False

    try:
        _validate_api_base(
            api_base,
            allow_production_8001=allow_production_8001,
            allow_non_localhost=allow_non_localhost,
        )
    except SmokeConfigError as exc:
        failures.append(str(exc))
        payload = _payload(started_at, api_base, endpoints, failures, counts, output, production_8001_touched)
        _write_json(output, payload)
        print(output)
        print(f"validation-center readonly smoke: failed - {exc}")
        return 1
    production_8001_touched = _is_production_8001(api_base)

    health = _extract_data(_append_endpoint(endpoints, _call(api_base, "/validation/health", timeout=timeout)), failures)
    if health:
        if health.get("mode") != "read_only":
            failures.append("/validation/health mode is not read_only")
        if health.get("production_8001_touched") is not False:
            failures.append("/validation/health production_8001_touched must be false")
        if not _is_object(health.get("history")):
            failures.append("/validation/health history must be an object")
        if not _is_object(health.get("plan_catalog")):
            failures.append("/validation/health plan_catalog must be an object")
        if not _is_object(health.get("quality")):
            failures.append("/validation/health quality must be an object")
        if not _is_object(health.get("runner")):
            failures.append("/validation/health runner must be an object")
        counts["health"] = {
            "run_count": (health.get("history") or {}).get("run_count") if isinstance(health.get("history"), dict) else None,
            "finding_count": (health.get("quality") or {}).get("finding_count") if isinstance(health.get("quality"), dict) else None,
            "bug_count": (health.get("quality") or {}).get("bug_count") if isinstance(health.get("quality"), dict) else None,
            "runner_job_count": (health.get("runner") or {}).get("job_count") if isinstance(health.get("runner"), dict) else None,
        }

    summary = _extract_data(_append_endpoint(endpoints, _call(api_base, "/validation/summary", timeout=timeout)), failures)
    if summary:
        for key in ("run_count", "coverage_snapshot_count", "evidence_manifest_count", "plan_count"):
            if not isinstance(summary.get(key), int):
                failures.append(f"/validation/summary {key} must be an integer")
        if not _is_object(summary.get("quality")):
            failures.append("/validation/summary quality must be an object")
        if not _is_object(summary.get("runner")):
            failures.append("/validation/summary runner must be an object")
        counts["summary"] = {
            "run_count": summary.get("run_count"),
            "coverage_snapshot_count": summary.get("coverage_snapshot_count"),
            "evidence_manifest_count": summary.get("evidence_manifest_count"),
            "plan_count": summary.get("plan_count"),
            "quality": summary.get("quality"),
            "runner": summary.get("runner"),
        }

    plans = _extract_data(_append_endpoint(endpoints, _call(api_base, "/validation/plans", timeout=timeout)), failures)
    if plans:
        plan_items = plans.get("plans")
        if not isinstance(plan_items, list):
            failures.append("/validation/plans plans must be a list")
        counts["plan_count"] = len(plan_items) if isinstance(plan_items, list) else None

    run_page = _check_page(api_base, "/validation/runs", "runs", endpoints, failures, counts, timeout, page_size)
    run_id = _first_id(run_page or {}, "run_id")
    if run_id:
        run_detail = _extract_data(_append_endpoint(endpoints, _call(api_base, f"/validation/runs/{_quote(run_id)}", timeout=timeout)), failures)
        if run_detail and run_detail.get("run_id") != run_id:
            failures.append("/validation/runs/{run_id} returned mismatched run_id")

    coverage_page = _check_page(api_base, "/validation/coverage", "coverage", endpoints, failures, counts, timeout, page_size)
    snapshot_id = _first_id(coverage_page or {}, "snapshot_id")
    if snapshot_id:
        coverage_detail = _extract_data(_append_endpoint(endpoints, _call(api_base, f"/validation/coverage/{_quote(snapshot_id)}", timeout=timeout)), failures)
        if coverage_detail and not (_is_object(coverage_detail.get("summary")) and _is_object(coverage_detail.get("snapshot"))):
            failures.append("/validation/coverage/{snapshot_id} must include summary and snapshot objects")

    evidence_page = _check_page(api_base, "/validation/evidence", "evidence", endpoints, failures, counts, timeout, page_size)
    manifest_id = _first_id(evidence_page or {}, "manifest_id")
    if manifest_id:
        evidence_detail = _extract_data(_append_endpoint(endpoints, _call(api_base, f"/validation/evidence/{_quote(manifest_id)}", timeout=timeout)), failures)
        if evidence_detail and not (_is_object(evidence_detail.get("summary")) and _is_object(evidence_detail.get("manifest"))):
            failures.append("/validation/evidence/{manifest_id} must include summary and manifest objects")

    execution_page = _check_page(api_base, "/validation/executions", "executions", endpoints, failures, counts, timeout, page_size)
    job_id = _first_id(execution_page or {}, "job_id")
    if job_id:
        execution_detail = _extract_data(_append_endpoint(endpoints, _call(api_base, f"/validation/executions/{_quote(job_id)}", timeout=timeout)), failures)
        if execution_detail and execution_detail.get("job_id") != job_id:
            failures.append("/validation/executions/{job_id} returned mismatched job_id")

    finding_summary = _extract_data(_append_endpoint(endpoints, _call(api_base, "/validation/findings/summary", timeout=timeout)), failures)
    if finding_summary and not isinstance(finding_summary.get("finding_count"), int):
        failures.append("/validation/findings/summary finding_count must be an integer")
    finding_page = _check_page(api_base, "/validation/findings", "findings", endpoints, failures, counts, timeout, page_size)
    finding_id = _first_id(finding_page or {}, "finding_id")
    if finding_id:
        finding_detail = _extract_data(_append_endpoint(endpoints, _call(api_base, f"/validation/findings/{_quote(finding_id)}", timeout=timeout)), failures)
        if finding_detail and not _is_object(finding_detail.get("agent_context")):
            failures.append("/validation/findings/{finding_id} must include agent_context")

    bug_summary = _extract_data(_append_endpoint(endpoints, _call(api_base, "/validation/bugs/summary", timeout=timeout)), failures)
    if bug_summary and not isinstance(bug_summary.get("bug_count"), int):
        failures.append("/validation/bugs/summary bug_count must be an integer")
    bug_page = _check_page(api_base, "/validation/bugs", "bugs", endpoints, failures, counts, timeout, page_size)
    bug_id = _first_id(bug_page or {}, "bug_id")
    if bug_id:
        bug_detail = _extract_data(_append_endpoint(endpoints, _call(api_base, f"/validation/bugs/{_quote(bug_id)}", timeout=timeout)), failures)
        if bug_detail and not _is_object(bug_detail.get("agent_context")):
            failures.append("/validation/bugs/{bug_id} must include agent_context")
        bug_context = _extract_data(_append_endpoint(endpoints, _call(api_base, f"/validation/bugs/{_quote(bug_id)}/agent-context", timeout=timeout)), failures)
        if bug_context and bug_context.get("context_type") != "bug":
            failures.append("/validation/bugs/{bug_id}/agent-context context_type must be bug")

    payload = _payload(started_at, api_base, endpoints, failures, counts, output, production_8001_touched)
    _write_json(output, payload)
    print(output)
    print(
        "validation-center readonly smoke: "
        f"{payload['status']} - endpoints={len(endpoints)} failures={len(failures)}"
    )
    for failure in failures:
        print(f"failure: {failure}")
    return 1 if failures else 0


def _check_page(
    api_base: str,
    path: str,
    count_key: str,
    endpoints: list[dict[str, Any]],
    failures: list[str],
    local_counts: dict[str, Any],
    timeout: float,
    page_size: int,
) -> dict[str, Any] | None:
    data = _extract_data(
        _append_endpoint(endpoints, _call(api_base, f"{path}?page=1&page_size={page_size}", timeout=timeout)),
        failures,
    )
    if data is None:
        return None
    if not _is_page(data):
        failures.append(f"{path} must return a validation page")
        return data
    local_counts[count_key] = data.get("total")
    return data


def _payload(
    started_at: str,
    api_base: str,
    endpoints: list[dict[str, Any]],
    failures: list[str],
    local_counts: dict[str, Any],
    output: Path,
    production_8001_touched: bool,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "started_at": started_at,
        "finished_at": _now_iso(),
        "status": "failed" if failures else "passed",
        "api_base": api_base,
        "output_path": _repo_path(output),
        "read_only": True,
        "write_methods_sent": [],
        "production_8001_touched": production_8001_touched,
        "endpoint_count": len(endpoints),
        "endpoints": endpoints,
        "counts": local_counts,
        "failures": failures,
        "failure_count": len(failures),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run read-only smoke checks against a live Validation Center API.")
    parser.add_argument("--api-base", default=_default_api_base())
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--page-size", type=int, default=5)
    parser.add_argument(
        "--allow-production-8001",
        action="store_true",
        help="Explicitly allow probing port 8001. This should not be used in normal development validation.",
    )
    parser.add_argument(
        "--allow-non-localhost",
        action="store_true",
        help="Explicitly allow probing a non-localhost API. This should not be used for normal development validation.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run_smoke(
        api_base=args.api_base,
        output=Path(args.output),
        timeout=args.timeout,
        page_size=args.page_size,
        allow_production_8001=args.allow_production_8001,
        allow_non_localhost=args.allow_non_localhost,
    )


if __name__ == "__main__":
    raise SystemExit(main())
