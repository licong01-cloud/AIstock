from __future__ import annotations

import json
from pathlib import Path

from scripts import strategy_package_governance_readonly_smoke as smoke


def _ok(payload: dict) -> tuple[int, dict, None]:
    return 200, {"ok": True, **payload}, None


def _route(path: str) -> tuple[int | None, dict | None, str | None]:
    path_only = path.split("?", 1)[0]
    routes: dict[str, tuple[int, dict, None]] = {
        "/strategy-packages/qe-sources": _ok({"sources": [{"source_id": "exp_1"}]}),
        "/strategy-packages": _ok(
            {
                "packages": [
                    {
                        "package_id": "pkg_1",
                        "manifest": {"schema_version": "strategy_package_manifest_v1"},
                        "metrics_summary": {"annual_return": 0.1},
                    }
                ]
            }
        ),
        "/strategy-packages/pkg_1": _ok(
            {
                "package": {
                    "package_id": "pkg_1",
                    "manifest": {"schema_version": "strategy_package_manifest_v1"},
                    "metrics_summary": {"annual_return": 0.1},
                }
            }
        ),
        "/strategy-packages/pkg_1/status-events": _ok({"events": [{"event_id": "event_1"}]}),
        "/strategy-packages/pkg_1/assets": _ok({"assets": [{"asset_id": 1}]}),
        "/strategy-packages/pkg_1/metrics-summary": _ok({"metrics_summary": {"annual_return": 0.1}}),
        "/strategy-packages/pkg_1/execution-policies": _ok({"execution_policies": [{"policy_id": "policy_1"}]}),
        "/strategy-packages/pkg_1/model-retrain/jobs": _ok({"jobs": []}),
        "/strategy-packages/pkg_1/selection-artifacts": _ok({"selection_artifacts": []}),
        "/strategy-packages/pkg_1/runtime-variants": _ok({"runtime_variants": [{"variant_id": "rtv_1"}]}),
        "/strategy-packages/pkg_1/validation-runs": _ok(
            {"validation_runs": [{"validation_run_id": "vr_1", "status": "PASSED"}]}
        ),
        "/strategy-packages/pkg_1/validation-runs/vr_1": _ok(
            {"validation_run": {"validation_run_id": "vr_1", "status": "PASSED"}}
        ),
        "/strategy-packages/pkg_1/validation-stability": _ok({"stability": {"package_id": "pkg_1"}}),
    }
    if path_only not in routes:
        return None, None, f"unexpected path: {path}"
    return routes[path_only]


def test_readonly_smoke_passes_with_complete_governance_contract(monkeypatch, tmp_path: Path) -> None:
    methods: list[str] = []

    def request_json(_api_base: str, path: str, *, timeout: float):
        methods.append("GET")
        return _route(path)

    monkeypatch.setattr(smoke, "_request_json", request_json)
    output = tmp_path / "strategy_package_smoke.json"

    exit_code = smoke.run_smoke(api_base="http://127.0.0.1:8011/api/v1", output=output)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["schema_version"] == smoke.SCHEMA_VERSION
    assert payload["status"] == "passed"
    assert payload["read_only"] is True
    assert payload["write_methods_sent"] == []
    assert payload["production_8001_touched"] is False
    assert payload["counts"]["packages"] == 1
    assert payload["counts"]["runtime_variants"] == 1
    assert payload["counts"]["validation_runs"] == 1
    assert payload["endpoint_count"] == 13
    assert set(methods) == {"GET"}


def test_readonly_smoke_passes_when_no_packages_exist(monkeypatch, tmp_path: Path) -> None:
    def route(path: str):
        path_only = path.split("?", 1)[0]
        if path_only == "/strategy-packages/qe-sources":
            return _ok({"sources": []})
        if path_only == "/strategy-packages":
            return _ok({"packages": []})
        return None, None, f"unexpected path: {path}"

    monkeypatch.setattr(smoke, "_request_json", lambda _api_base, path, *, timeout: route(path))
    output = tmp_path / "empty.json"

    exit_code = smoke.run_smoke(api_base="http://127.0.0.1:8011/api/v1", output=output)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["counts"]["packages"] == 0
    assert payload["endpoint_count"] == 2


def test_readonly_smoke_blocks_production_8001(tmp_path: Path) -> None:
    output = tmp_path / "blocked.json"

    exit_code = smoke.run_smoke(api_base="http://127.0.0.1:8001/api/v1", output=output)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert "refusing to touch production backend port 8001" in payload["failures"]
    assert payload["endpoint_count"] == 0


def test_readonly_smoke_records_explicit_production_probe(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(smoke, "_request_json", lambda _api_base, path, *, timeout: _route(path))
    output = tmp_path / "allowed_production.json"

    exit_code = smoke.run_smoke(
        api_base="http://127.0.0.1:8001/api/v1",
        output=output,
        allow_production_8001=True,
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["production_8001_touched"] is True


def test_readonly_smoke_blocks_non_localhost_by_default(tmp_path: Path) -> None:
    output = tmp_path / "remote.json"

    exit_code = smoke.run_smoke(api_base="http://192.0.2.10:8011/api/v1", output=output)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert "refusing to touch non-localhost StrategyPackage API" in payload["failures"]
    assert payload["endpoint_count"] == 0


def test_readonly_smoke_fails_on_missing_governance_list(monkeypatch, tmp_path: Path) -> None:
    def route(path: str):
        path_only = path.split("?", 1)[0]
        if path_only == "/strategy-packages/qe-sources":
            return _ok({"sources": []})
        if path_only == "/strategy-packages":
            return _ok({"packages": [{"package_id": "pkg_1", "manifest": {}, "metrics_summary": {}}]})
        if path_only == "/strategy-packages/pkg_1/runtime-variants":
            return _ok({"runtime_variants": {}})
        return _route(path)

    monkeypatch.setattr(smoke, "_request_json", lambda _api_base, path, *, timeout: route(path))
    output = tmp_path / "bad_contract.json"

    exit_code = smoke.run_smoke(api_base="http://127.0.0.1:8011/api/v1", output=output)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert "/strategy-packages/pkg_1/runtime-variants runtime_variants must be a list" in payload["failures"]
