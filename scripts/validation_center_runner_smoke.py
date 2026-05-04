from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "aistock_validation_center_runner_smoke_v1"
DEFAULT_OUTPUT = ROOT / "tmp" / "validation" / "validation_center" / "runner_smoke.json"
TERMINAL_STATUSES = {"passed", "failed", "timeout", "rejected"}


class RunnerSmokeConfigError(ValueError):
    """Raised when runner smoke configuration would touch a forbidden endpoint."""


def _now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _default_api_base() -> str:
    raw = os.environ.get("VALIDATION_CENTER_API_BASE") or os.environ.get("NEXT_PUBLIC_API_BASE")
    if raw:
        return raw
    backend_port = os.environ.get("BACKEND_PORT", "8012")
    return f"http://127.0.0.1:{backend_port}/api/v1"


def _validate_api_base(api_base: str, *, allow_production_8001: bool, allow_non_localhost: bool) -> None:
    parsed = urllib.parse.urlparse(api_base)
    if parsed.scheme not in {"http", "https"}:
        raise RunnerSmokeConfigError(f"api base must use http/https: {api_base}")
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"} and not allow_non_localhost:
        raise RunnerSmokeConfigError("refusing to touch non-localhost validation API")
    if parsed.port == 8001 and not allow_production_8001:
        raise RunnerSmokeConfigError("refusing to touch production backend port 8001")


def _url(api_base: str, path: str) -> str:
    return f"{api_base.rstrip('/')}/{path.lstrip('/')}"


def _request_json(
    api_base: str,
    path: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    timeout: float = 5.0,
) -> tuple[int | None, dict[str, Any] | None, str | None]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        _url(api_base, path),
        data=data,
        method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            text = response.read().decode("utf-8")
            payload = json.loads(text) if text.strip() else {}
            if not isinstance(payload, dict):
                return response.status, None, "JSON root is not an object"
            return response.status, payload, None
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        return exc.code, None, f"HTTP {exc.code}: {text or exc.reason}"
    except urllib.error.URLError as exc:
        return None, None, f"connection failed: {exc.reason}"
    except TimeoutError:
        return None, None, "timeout"
    except json.JSONDecodeError as exc:
        return None, None, f"invalid JSON: {exc}"
    except UnicodeDecodeError as exc:
        return None, None, f"invalid UTF-8: {exc}"


def _extract_data(result: dict[str, Any], failures: list[str]) -> dict[str, Any] | None:
    if not result["ok"]:
        failures.append(f"{result['method']} {result['path']} failed: {result.get('error') or result.get('status_code')}")
        return None
    payload = result.get("payload")
    if not isinstance(payload, dict) or payload.get("status") != "success" or not isinstance(payload.get("data"), dict):
        failures.append(f"{result['method']} {result['path']} returned invalid validation envelope")
        return None
    return payload["data"]


def _call(
    api_base: str,
    path: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    timeout: float,
) -> dict[str, Any]:
    status_code, payload, error = _request_json(api_base, path, method=method, body=body, timeout=timeout)
    return {
        "method": method,
        "path": path,
        "status_code": status_code,
        "ok": error is None and status_code is not None and 200 <= status_code < 300,
        "error": error,
        "payload": payload,
    }


def run_smoke(
    *,
    api_base: str,
    output: Path,
    plan_key: str = "guardrail_changed_files",
    requested_by: str = "runner_smoke",
    timeout_seconds: int = 120,
    poll_timeout_seconds: int = 180,
    poll_interval_seconds: float = 1.0,
    allow_production_8001: bool = False,
    allow_non_localhost: bool = False,
) -> int:
    started_at = _now_iso()
    api_base = api_base.strip().rstrip("/")
    endpoints: list[dict[str, Any]] = []
    failures: list[str] = []
    write_methods_sent: list[str] = []
    job: dict[str, Any] | None = None
    log: dict[str, Any] | None = None
    evidence: dict[str, Any] | None = None
    run_detail: dict[str, Any] | None = None

    try:
        _validate_api_base(
            api_base,
            allow_production_8001=allow_production_8001,
            allow_non_localhost=allow_non_localhost,
        )
    except RunnerSmokeConfigError as exc:
        failures.append(str(exc))
        payload = _payload(started_at, api_base, endpoints, failures, write_methods_sent, job, log, evidence, run_detail)
        _write_json(output, payload)
        print(output)
        print(f"validation-center runner smoke: failed - {exc}")
        return 1

    start_body = {"plan_key": plan_key, "requested_by": requested_by, "timeout_seconds": timeout_seconds}
    start_result = _call(api_base, "/validation/executions", method="POST", body=start_body, timeout=timeout_seconds)
    endpoints.append(_endpoint_summary(start_result))
    write_methods_sent.append("POST /validation/executions")
    job = _extract_data(start_result, failures)
    job_id = str((job or {}).get("job_id") or "")
    if job_id:
        deadline = time.monotonic() + poll_timeout_seconds
        while time.monotonic() <= deadline:
            detail_result = _call(api_base, f"/validation/executions/{urllib.parse.quote(job_id, safe='')}", timeout=timeout_seconds)
            endpoints.append(_endpoint_summary(detail_result))
            detail = _extract_data(detail_result, failures)
            if detail:
                job = detail
                if str(detail.get("status") or "") in TERMINAL_STATUSES:
                    break
            time.sleep(poll_interval_seconds)
        if str((job or {}).get("status") or "") not in TERMINAL_STATUSES:
            failures.append(f"runner job did not finish before timeout: {job_id}")
        log_result = _call(api_base, f"/validation/executions/{urllib.parse.quote(job_id, safe='')}/log?tail_lines=120", timeout=timeout_seconds)
        endpoints.append(_endpoint_summary(log_result))
        log = _extract_data(log_result, failures)
        evidence_result = _call(api_base, f"/validation/executions/{urllib.parse.quote(job_id, safe='')}/evidence", timeout=timeout_seconds)
        endpoints.append(_endpoint_summary(evidence_result))
        evidence = _extract_data(evidence_result, failures)
        run_id = ((job or {}).get("archive") or {}).get("run_id") if isinstance((job or {}).get("archive"), dict) else None
        if run_id:
            run_result = _call(api_base, f"/validation/runs/{urllib.parse.quote(str(run_id), safe='')}", timeout=timeout_seconds)
            endpoints.append(_endpoint_summary(run_result))
            run_detail = _extract_data(run_result, failures)
        else:
            failures.append("runner job did not provide archive.run_id")

    if job and job.get("status") != "passed":
        failures.append(f"runner job status is not passed: {job.get('status')}")
    if job and (job.get("archive") or {}).get("status") != "archived":
        failures.append("runner job archive status is not archived")
    if evidence and not isinstance(evidence.get("standard_evidence"), dict):
        failures.append("runner evidence endpoint did not return standard_evidence")
    if run_detail and run_detail.get("metadata_missing"):
        failures.append("archived validation run metadata is missing")

    payload = _payload(started_at, api_base, endpoints, failures, write_methods_sent, job, log, evidence, run_detail)
    _write_json(output, payload)
    print(output)
    print(
        "validation-center runner smoke: "
        f"{payload['status']} - endpoints={len(endpoints)} failures={len(failures)}"
    )
    for failure in failures:
        print(f"failure: {failure}")
    return 1 if failures else 0


def _endpoint_summary(result: dict[str, Any]) -> dict[str, Any]:
    stored = dict(result)
    payload = stored.pop("payload", None)
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict):
        stored["data_keys"] = sorted(str(key) for key in data.keys())
    return stored


def _payload(
    started_at: str,
    api_base: str,
    endpoints: list[dict[str, Any]],
    failures: list[str],
    write_methods_sent: list[str],
    job: dict[str, Any] | None,
    log: dict[str, Any] | None,
    evidence: dict[str, Any] | None,
    run_detail: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "started_at": started_at,
        "finished_at": _now_iso(),
        "status": "failed" if failures else "passed",
        "api_base": api_base,
        "write_methods_sent": write_methods_sent,
        "production_8001_touched": urllib.parse.urlparse(api_base).port == 8001,
        "endpoint_count": len(endpoints),
        "endpoints": endpoints,
        "job": job,
        "log": log,
        "evidence": evidence,
        "run_detail": run_detail,
        "failures": failures,
        "failure_count": len(failures),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a controlled Validation Center runner smoke against a dev API.")
    parser.add_argument("--api-base", default=_default_api_base())
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--plan-key", default="guardrail_changed_files")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--poll-timeout-seconds", type=int, default=180)
    parser.add_argument("--poll-interval-seconds", type=float, default=1.0)
    parser.add_argument("--allow-production-8001", action="store_true")
    parser.add_argument("--allow-non-localhost", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run_smoke(
        api_base=args.api_base,
        output=Path(args.output),
        plan_key=args.plan_key,
        timeout_seconds=args.timeout_seconds,
        poll_timeout_seconds=args.poll_timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        allow_production_8001=args.allow_production_8001,
        allow_non_localhost=args.allow_non_localhost,
    )


if __name__ == "__main__":
    raise SystemExit(main())
