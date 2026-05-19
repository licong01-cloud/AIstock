from __future__ import annotations

import json
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.validation.platform_health import ValidationPlatformHealthService


_ROUTER_PATH = Path(__file__).resolve().parents[1] / "routers" / "validation.py"
_ROUTER_SPEC = spec_from_file_location("backend.routers.validation", _ROUTER_PATH)
assert _ROUTER_SPEC is not None and _ROUTER_SPEC.loader is not None
validation = module_from_spec(_ROUTER_SPEC)
sys.modules[_ROUTER_SPEC.name] = validation
_ROUTER_SPEC.loader.exec_module(validation)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_catalog(path: Path, *, command_key: str = "nox_validation_center_backend") -> None:
    _write_json(
        path,
        {
            "schema_version": "aistock_validation_plans_v1",
            "plans": [
                {
                    "plan_key": "validation_center_backend",
                    "title": "Validation Center backend",
                    "module": "validation_center",
                    "level": "L2",
                    "command_key": command_key,
                    "nox_session": "validation_center_backend",
                    "enabled": True,
                    "requires_backend": True,
                    "requires_frontend": False,
                    "allowed_backend_ports": [8011],
                    "allowed_frontend_ports": [],
                    "writes_database": False,
                    "writes_artifacts": False,
                    "writes_business_state": False,
                }
            ],
        },
    )


def _write_repo_fixture(root: Path, *, with_nightly: bool = True) -> None:
    (root / ".github" / "workflows").mkdir(parents=True, exist_ok=True)
    (root / "tests" / "aistock_validation" / "catalog").mkdir(parents=True, exist_ok=True)
    (root / "tests" / "aistock_validation" / "history").mkdir(parents=True, exist_ok=True)
    (root / "tests" / "aistock_validation" / "catalog" / "module_registry.yaml").write_text(
        "\n".join(
            [
                "schema_version: aistock_module_registry_v1",
                "modules:",
                "  - module_id: validation",
                "    display_name: Validation",
                "    module_type: cross_cutting",
                "    risk_level: medium",
                "  - module_id: validation.center",
                "    display_name: Validation Center",
                "    parent_module: validation",
                "    module_type: product_feature",
                "    risk_level: medium",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "tests" / "aistock_validation" / "catalog" / "ui_targets.yaml").write_text(
        "\n".join(
            [
                "schema_version: aistock_validation_ui_targets_v1",
                "targets:",
                "  - route_id: validation.center",
                "    href: /validation-center",
                "    label: Validation Center",
                "    nav_group: Validation Pipeline",
                "    primary_module: validation.center",
                "    impact_modules: []",
                "    risk_level: medium",
                "    required_test_plans: [validation_center_backend]",
                "    recommended_test_plans: []",
                "    coverage_status: partial",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "tests" / "aistock_validation" / "catalog" / "file_ownership.yaml").write_text(
        "schema_version: aistock_file_ownership_v1\nrules: []\n",
        encoding="utf-8",
    )
    (root / "tests" / "aistock_validation" / "catalog" / "resource_policies.yaml").write_text(
        "schema_version: aistock_validation_resource_policies_v1\npolicies: {}\n",
        encoding="utf-8",
    )
    (root / "frontend" / "src" / "lib" / "navigation").mkdir(parents=True, exist_ok=True)
    (root / "frontend" / "src" / "lib" / "navigation" / "nav-groups.ts").write_text(
        'export const NAV_GROUPS = [{ title: "Validation", items: [{ href: "/validation-center", label: "Validation Center" }] }];\n',
        encoding="utf-8",
    )
    (root / "noxfile.py").write_text(
        "import nox\n\n@nox.session(venv_backend='none')\ndef validation_center_backend(session):\n    pass\n",
        encoding="utf-8",
    )
    _write_catalog(root / "tests" / "aistock_validation" / "catalog" / "test_plans.yaml")
    if with_nightly:
        (root / ".github" / "workflows" / "nightly.yml").write_text(
            "\n".join(
                [
                    "name: AIstock Nightly L3 + DR",
                    "on:",
                    "  schedule:",
                    "    - cron: '7 19 * * *'",
                    "jobs:",
                    "  nightly:",
                    "    runs-on: [self-hosted, windows]",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
    (root / "tests" / "aistock_validation" / "history" / "20260518_nightly_summary.md").write_text(
        "# Nightly summary\n\n- status: queued\n",
        encoding="utf-8",
    )


def _make_runner(*, gh_available: bool = True, runner_online: bool = False):
    gh_runs = json.dumps(
        [
            {
                "databaseId": 26059234354,
                "status": "queued",
                "conclusion": None,
                "createdAt": "2026-05-18T20:41:17Z",
                "updatedAt": "2026-05-18T22:41:17Z",
                "url": "https://github.com/example/aistock/actions/runs/26059234354",
                "workflowName": "AIstock Nightly L3 + DR",
                "headBranch": "main",
                "headSha": "abc1234",
                "displayTitle": "Nightly run",
            }
        ],
        ensure_ascii=False,
    )
    gh_run_view = json.dumps(
        {
            "jobs": [
                {
                    "name": "DR snapshot prod DB",
                    "status": "queued",
                    "conclusion": None,
                    "startedAt": None,
                    "completedAt": None,
                    "runnerName": None,
                    "labels": [{"name": "self-hosted"}, {"name": "windows"}],
                    "url": "https://github.com/example/aistock/actions/jobs/1",
                }
            ],
            "status": "queued",
            "conclusion": None,
            "createdAt": "2026-05-18T20:41:17Z",
            "updatedAt": "2026-05-18T22:41:17Z",
            "url": "https://github.com/example/aistock/actions/runs/26059234354",
            "workflowName": "AIstock Nightly L3 + DR",
            "displayTitle": "Nightly run",
        },
        ensure_ascii=False,
    )
    gh_runners = json.dumps(
        {
            "total_count": 1,
            "runners": [
                {
                    "id": 1,
                    "name": "win-runner-1",
                    "status": "online" if runner_online else "offline",
                    "busy": runner_online,
                    "os": "Windows",
                    "labels": [{"name": "self-hosted"}, {"name": "windows"}],
                }
            ],
        },
        ensure_ascii=False,
    )

    def run(args: list[str], cwd: Path, timeout: int):  # noqa: ARG001
        if args[:3] == ["git", "rev-parse", "--show-toplevel"]:
            return 0, str(cwd) + "\n", ""
        if args == ["git", "rev-parse", "--is-inside-work-tree"]:
            return 0, "true\n", ""
        if args == ["git", "branch", "--show-current"]:
            return 0, "main\n", ""
        if args == ["git", "rev-parse", "HEAD"]:
            return 0, "abc1234567890\n", ""
        if args[:3] == ["git", "status", "--porcelain=v1"]:
            return 0, "", ""
        if args[:4] == ["git", "rev-list", "--left-right", "--count"]:
            return 0, "0\t0\n", ""
        if args == ["git", "config", "--get", "remote.origin.url"]:
            return 0, "https://github.com/example/aistock.git\n", ""
        if args == ["gh", "auth", "status", "--hostname", "github.com"]:
            if gh_available:
                return 0, "github.com\n  X Logged in to github.com as example (token)\n", ""
            return 1, "", "gh: not logged in"
        if args[:4] == ["gh", "run", "list", "--workflow"]:
            if gh_available:
                return 0, gh_runs, ""
            return 1, "", "gh run list unavailable"
        if args[:4] == ["gh", "run", "view", "26059234354"]:
            if gh_available:
                return 0, gh_run_view, ""
            return 1, "", "gh run view unavailable"
        if args[:3] == ["gh", "api", "repos/example/aistock/actions/runners"]:
            if gh_available:
                return 0, gh_runners, ""
            return 1, "", "gh api unavailable"
        return 1, "", f"unexpected command: {args}"

    return run


def test_platform_health_surfaces_nightly_and_runner_state(tmp_path: Path) -> None:
    _write_repo_fixture(tmp_path)
    service = ValidationPlatformHealthService(repo_root=tmp_path, env={}, command_runner=_make_runner())

    payload = service.summary()

    assert payload["schema_version"].startswith("aistock_validation_platform_health")
    assert payload["state"] == "degraded"
    assert payload["repo_context"]["state"] == "healthy"
    assert payload["catalog_integrity"]["state"] == "healthy"
    assert payload["github_connectivity"]["state"] == "healthy"
    assert payload["nightly_summary"]["state"] == "blocked"
    assert payload["nightly_summary"]["latest_run"]["run_id"] == 26059234354
    assert payload["nightly_summary"]["jobs"][0]["name"] == "DR snapshot prod DB"
    assert payload["runner_health"]["github_runner"]["state"] == "blocked"
    assert payload["runner_health"]["github_runner"]["matching_runner_count"] == 1
    assert payload["runner_health"]["github_runner"]["online_count"] == 0
    assert payload["reason_codes"]
    assert payload["production_8001_touched"] is False


def test_platform_health_degrades_without_github_token(tmp_path: Path) -> None:
    _write_repo_fixture(tmp_path)
    service = ValidationPlatformHealthService(repo_root=tmp_path, env={}, command_runner=_make_runner(gh_available=False))

    payload = service.summary()

    assert payload["state"] == "degraded"
    assert payload["github_connectivity"]["data_state"] == "unavailable"
    assert payload["runner_health"]["state"] == "unavailable"
    assert payload["nightly_summary"]["data_state"] == "unavailable"
    assert "gh_auth_unavailable" in payload["reason_codes"]


def test_platform_health_blocks_dirty_repo_without_500(tmp_path: Path) -> None:
    _write_repo_fixture(tmp_path)

    def runner(args: list[str], cwd: Path, timeout: int):  # noqa: ARG001
        if args[:3] == ["git", "rev-parse", "--show-toplevel"]:
            return 0, str(cwd) + "\n", ""
        if args == ["git", "rev-parse", "--is-inside-work-tree"]:
            return 0, "true\n", ""
        if args == ["git", "branch", "--show-current"]:
            return 0, "main\n", ""
        if args == ["git", "rev-parse", "HEAD"]:
            return 0, "abc1234567890\n", ""
        if args[:3] == ["git", "status", "--porcelain=v1"]:
            return 0, " M backend/services/validation/platform_health.py\n?? tmp.txt\n", ""
        if args[:4] == ["git", "rev-list", "--left-right", "--count"]:
            return 0, "0\t0\n", ""
        if args == ["git", "config", "--get", "remote.origin.url"]:
            return 0, "https://github.com/example/aistock.git\n", ""
        if args == ["gh", "auth", "status", "--hostname", "github.com"]:
            return 1, "", "gh: not logged in"
        return 1, "", f"unexpected command: {args}"

    service = ValidationPlatformHealthService(repo_root=tmp_path, env={}, command_runner=runner)

    payload = service.summary()

    assert payload["state"] == "blocked"
    assert payload["repo_context"]["state"] == "blocked"
    assert "repo_dirty" in payload["repo_context"]["reason_codes"]
    assert payload["repo_context"]["untracked_count"] == 1


def test_platform_health_endpoint_is_read_only(tmp_path: Path) -> None:
    class DummyPipelineCenter:
        def platform_health_summary(self) -> dict[str, object]:
            return {"schema_version": "aistock_validation_platform_health_v1", "state": "healthy", "data_state": "complete"}

        def catalog_integrity_summary(self) -> dict[str, object]:
            return {"schema_version": "aistock_validation_catalog_integrity_v1", "state": "passed"}

        def nightly_summary(self) -> dict[str, object]:
            return {"schema_version": "aistock_validation_nightly_summary_v1", "state": "healthy"}

        def nightly_runs(self, *, limit: int = 10) -> dict[str, object]:
            return {"items": [{"run_id": 1}], "total": 1, "page": 1, "page_size": limit, "has_more": False}

        def nightly_runner_health(self) -> dict[str, object]:
            return {"schema_version": "aistock_validation_runner_health_v1", "state": "healthy"}

    app = FastAPI()
    app.include_router(validation.router, prefix="/api/v1")
    app.dependency_overrides[validation.get_pipeline_center_service] = lambda: DummyPipelineCenter()
    client = TestClient(app)

    response = client.get("/api/v1/validation/platform/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "success"
    assert payload["data"]["state"] == "healthy"

    assert client.get("/api/v1/validation/catalog/integrity").json()["data"]["state"] == "passed"
    assert client.get("/api/v1/validation/nightly/summary").json()["data"]["state"] == "healthy"
    assert client.get("/api/v1/validation/nightly/runs").json()["data"]["total"] == 1
    assert client.get("/api/v1/validation/nightly/runner-health").json()["data"]["state"] == "healthy"
