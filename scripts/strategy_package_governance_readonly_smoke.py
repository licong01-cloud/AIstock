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
SCHEMA_VERSION = "aistock_strategy_package_governance_readonly_smoke_v1"
DEFAULT_OUTPUT = ROOT / "tmp" / "validation" / "strategy_package" / "governance_readonly_smoke.json"
LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}


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
    raw = os.environ.get("STRATEGY_PACKAGE_API_BASE") or os.environ.get("NEXT_PUBLIC_API_BASE")
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
    if parsed.hostname not in LOCAL_HOSTS and not allow_non_localhost:
        raise SmokeConfigError("refusing to touch non-localhost StrategyPackage API")
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


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def _is_object(value: Any) -> bool:
    return isinstance(value, dict)


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
    if isinstance(payload, dict):
        stored["payload_keys"] = sorted(str(key) for key in payload.keys())
        for key in (
            "packages",
            "sources",
            "assets",
            "events",
            "runtime_variants",
            "validation_runs",
            "execution_policies",
            "jobs",
            "selection_artifacts",
        ):
            value = payload.get(key)
            if isinstance(value, list):
                stored[f"{key}_count"] = len(value)
    endpoints.append(stored)
    return result


def _extract_payload(result: dict[str, Any], failures: list[str]) -> dict[str, Any] | None:
    if not result["ok"]:
        failures.append(f"{result['path']} request failed: {result.get('error') or result.get('status_code')}")
        return None
    payload = result.get("payload")
    if not isinstance(payload, dict):
        failures.append(f"{result['path']} returned non-object payload")
        return None
    if payload.get("ok") is not True:
        failures.append(f"{result['path']} ok is not true: {payload.get('ok')}")
        return None
    return payload


def _first_id(items: Any, key: str) -> str | None:
    if not isinstance(items, list) or not items:
        return None
    first = items[0]
    if not isinstance(first, dict):
        return None
    value = first.get(key)
    return str(value) if value else None


def _require_list(payload: dict[str, Any], path: str, key: str, failures: list[str]) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        failures.append(f"{path} {key} must be a list")
        return []
    return value


def _require_object(payload: dict[str, Any], path: str, key: str, failures: list[str]) -> dict[str, Any] | None:
    value = payload.get(key)
    if not _is_object(value):
        failures.append(f"{path} {key} must be an object")
        return None
    return value


def _require_bool(payload: dict[str, Any], path: str, key: str, failures: list[str]) -> bool | None:
    value = payload.get(key)
    if not isinstance(value, bool):
        failures.append(f"{path} {key} must be a boolean")
        return None
    return value


def _check_payload_package(payload: dict[str, Any], path: str, failures: list[str]) -> tuple[str | None, str | None]:
    package = _require_object(payload, path, "package", failures)
    if package is None:
        return None, None
    package_id = package.get("package_id")
    if not package_id:
        failures.append(f"{path} package.package_id must be present")
        return None, None
    manifest_sha256 = package.get("manifest_sha256")
    if not isinstance(manifest_sha256, str) or not manifest_sha256:
        failures.append(f"{path} package.manifest_sha256 must be present")
        return None, None
    if not _is_object(package.get("manifest")):
        failures.append(f"{path} package.manifest must be an object")
    if not _is_object(package.get("metrics_summary")):
        failures.append(f"{path} package.metrics_summary must be an object")
    return str(package_id), manifest_sha256


def run_smoke(
    *,
    api_base: str,
    output: Path,
    timeout: float = 5.0,
    limit: int = 5,
    allow_production_8001: bool = False,
    allow_non_localhost: bool = False,
) -> int:
    started_at = _now_iso()
    endpoints: list[dict[str, Any]] = []
    failures: list[str] = []
    counts: dict[str, Any] = {}
    production_8001_touched = False

    try:
        api_base = _normalize_api_base(api_base)
        _validate_api_base(
            api_base,
            allow_production_8001=allow_production_8001,
            allow_non_localhost=allow_non_localhost,
        )
    except SmokeConfigError as exc:
        api_base = api_base if isinstance(api_base, str) else ""
        failures.append(str(exc))
        payload = _payload(started_at, api_base, endpoints, failures, counts, output, production_8001_touched)
        _write_json(output, payload)
        print(output)
        print(f"strategy-package governance readonly smoke: failed - {exc}")
        return 1
    production_8001_touched = _is_production_8001(api_base)

    sources = _extract_payload(
        _append_endpoint(endpoints, _call(api_base, f"/strategy-packages/qe-sources?source_kind=all&limit={limit}", timeout=timeout)),
        failures,
    )
    if sources:
        counts["qe_sources"] = len(_require_list(sources, "/strategy-packages/qe-sources", "sources", failures))

    package_list = _extract_payload(
        _append_endpoint(endpoints, _call(api_base, f"/strategy-packages?limit={limit}", timeout=timeout)),
        failures,
    )
    package_id: str | None = None
    package_manifest_sha256: str | None = None
    if package_list:
        packages = _require_list(package_list, "/strategy-packages", "packages", failures)
        counts["packages"] = len(packages)
        package_id = _first_id(packages, "package_id")
        if packages and package_id is None:
            failures.append("/strategy-packages first package must include package_id")

    if package_id:
        encoded_package_id = _quote(package_id)
        detail = _extract_payload(
            _append_endpoint(endpoints, _call(api_base, f"/strategy-packages/{encoded_package_id}", timeout=timeout)),
            failures,
        )
        if detail:
            detail_id, detail_manifest_sha256 = _check_payload_package(detail, f"/strategy-packages/{package_id}", failures)
            if detail_id and detail_id != package_id:
                failures.append("/strategy-packages/{package_id} returned mismatched package_id")
            if detail_manifest_sha256:
                package_manifest_sha256 = detail_manifest_sha256

        _check_list_endpoint(
            api_base,
            f"/strategy-packages/{encoded_package_id}/status-events?limit={limit}",
            "events",
            endpoints,
            failures,
            counts,
            timeout,
        )
        _check_list_endpoint(
            api_base,
            f"/strategy-packages/{encoded_package_id}/assets",
            "assets",
            endpoints,
            failures,
            counts,
            timeout,
        )
        _check_object_endpoint(
            api_base,
            f"/strategy-packages/{encoded_package_id}/metrics-summary",
            "metrics_summary",
            endpoints,
            failures,
            timeout,
        )
        _check_list_endpoint(
            api_base,
            f"/strategy-packages/{encoded_package_id}/execution-policies",
            "execution_policies",
            endpoints,
            failures,
            counts,
            timeout,
        )
        _check_list_endpoint(
            api_base,
            f"/strategy-packages/{encoded_package_id}/model-retrain/jobs?limit={limit}",
            "jobs",
            endpoints,
            failures,
            counts,
            timeout,
        )
        _check_list_endpoint(
            api_base,
            f"/strategy-packages/{encoded_package_id}/selection-artifacts?limit={limit}",
            "selection_artifacts",
            endpoints,
            failures,
            counts,
            timeout,
        )
        _check_list_endpoint(
            api_base,
            f"/strategy-packages/{encoded_package_id}/runtime-variants?limit={limit}",
            "runtime_variants",
            endpoints,
            failures,
            counts,
            timeout,
        )
        validation_runs = _check_list_endpoint(
            api_base,
            f"/strategy-packages/{encoded_package_id}/validation-runs?limit={limit}",
            "validation_runs",
            endpoints,
            failures,
            counts,
            timeout,
        )
        validation_run_id = _first_id(validation_runs, "validation_run_id")
        if validation_run_id:
            run_detail = _extract_payload(
                _append_endpoint(
                    endpoints,
                    _call(
                        api_base,
                        f"/strategy-packages/{encoded_package_id}/validation-runs/{_quote(validation_run_id)}",
                        timeout=timeout,
                    ),
                ),
                failures,
            )
            if run_detail:
                run = _require_object(run_detail, "/strategy-packages/{package_id}/validation-runs/{validation_run_id}", "validation_run", failures)
                if run and run.get("validation_run_id") != validation_run_id:
                    failures.append("/strategy-packages/{package_id}/validation-runs/{validation_run_id} returned mismatched validation_run_id")

        _check_object_endpoint(
            api_base,
            f"/strategy-packages/{encoded_package_id}/validation-stability?limit={limit}",
            "stability",
            endpoints,
            failures,
            timeout,
        )
        eligibility = _check_object_endpoint(
            api_base,
            f"/strategy-packages/{encoded_package_id}/governance-eligibility?metric_key={_quote('annual_return')}&limit={limit}",
            "eligibility",
            endpoints,
            failures,
            timeout,
        )
        if eligibility:
            if eligibility.get("package_id") != package_id:
                failures.append("/strategy-packages/{package_id}/governance-eligibility returned mismatched package_id")
            if package_manifest_sha256 and eligibility.get("manifest_sha256") != package_manifest_sha256:
                failures.append(
                    "/strategy-packages/{package_id}/governance-eligibility returned mismatched manifest_sha256"
                )
            _require_bool(eligibility, "/strategy-packages/{package_id}/governance-eligibility", "paper_ready", failures)
            _require_list(eligibility, "/strategy-packages/{package_id}/governance-eligibility", "blockers", failures)
            _require_list(
                eligibility,
                "/strategy-packages/{package_id}/governance-eligibility",
                "satisfied_gates",
                failures,
            )
            for key in (
                "manifest_identity",
                "original_fixed_weight_retest",
                "validation_stability",
                "protected_asset_status",
                "runtime_variant_candidate_status",
            ):
                _require_object(eligibility, "/strategy-packages/{package_id}/governance-eligibility", key, failures)

    payload = _payload(started_at, api_base, endpoints, failures, counts, output, production_8001_touched)
    _write_json(output, payload)
    print(output)
    print(
        "strategy-package governance readonly smoke: "
        f"{payload['status']} - endpoints={len(endpoints)} failures={len(failures)}"
    )
    for failure in failures:
        print(f"failure: {failure}")
    return 1 if failures else 0


def _check_list_endpoint(
    api_base: str,
    path: str,
    key: str,
    endpoints: list[dict[str, Any]],
    failures: list[str],
    counts: dict[str, Any],
    timeout: float,
) -> list[Any]:
    payload = _extract_payload(_append_endpoint(endpoints, _call(api_base, path, timeout=timeout)), failures)
    if payload is None:
        return []
    items = _require_list(payload, path.split("?", 1)[0], key, failures)
    counts[key] = len(items)
    return items


def _check_object_endpoint(
    api_base: str,
    path: str,
    key: str,
    endpoints: list[dict[str, Any]],
    failures: list[str],
    timeout: float,
) -> dict[str, Any] | None:
    payload = _extract_payload(_append_endpoint(endpoints, _call(api_base, path, timeout=timeout)), failures)
    if payload is None:
        return None
    return _require_object(payload, path.split("?", 1)[0], key, failures)


def _payload(
    started_at: str,
    api_base: str,
    endpoints: list[dict[str, Any]],
    failures: list[str],
    counts: dict[str, Any],
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
        "counts": counts,
        "failures": failures,
        "failure_count": len(failures),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run GET-only smoke checks against StrategyPackage governance APIs.")
    parser.add_argument("--api-base", default=_default_api_base())
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--limit", type=int, default=5)
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
        limit=args.limit,
        allow_production_8001=args.allow_production_8001,
        allow_non_localhost=args.allow_non_localhost,
    )


if __name__ == "__main__":
    raise SystemExit(main())
