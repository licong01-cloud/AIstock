from __future__ import annotations

import json
from pathlib import Path

from scripts import validation_center_readonly_smoke as smoke


def _envelope(data: dict) -> tuple[int, dict, None]:
    return 200, {"status": "success", "data": data}, None


def _page(items: list[dict], *, total: int | None = None) -> dict:
    return {
        "items": items,
        "total": len(items) if total is None else total,
        "page": 1,
        "page_size": 5,
        "has_more": False,
    }


def _route(path: str) -> tuple[int | None, dict | None, str | None]:
    path_only = path.split("?", 1)[0]
    routes: dict[str, tuple[int, dict, None]] = {
        "/validation/health": _envelope(
            {
                "mode": "read_only",
                "history": {"run_count": 1, "coverage_snapshot_count": 1, "evidence_manifest_count": 1},
                "plan_catalog": {"plan_count": 1},
                "quality": {"finding_count": 1, "bug_count": 1},
                "runner": {"job_count": 1, "production_8001_touched": False},
                "production_8001_touched": False,
            }
        ),
        "/validation/summary": _envelope(
            {
                "run_count": 1,
                "coverage_snapshot_count": 1,
                "evidence_manifest_count": 1,
                "plan_count": 1,
                "quality": {"finding_count": 1, "bug_count": 1},
                "runner": {"job_count": 1, "jobs_by_status": {"passed": 1}},
            }
        ),
        "/validation/plans": _envelope({"plans": [{"plan_key": "validation_center_backend"}]}),
        "/validation/cards/summary": _envelope({"cards": [{"card_id": "merge_gate", "title": "合入门禁"}]}),
        "/validation/merge-gate/summary": _envelope({"decision": "warning", "blocking_reasons": [], "warnings": []}),
        "/validation/merge-gate/detail": _envelope({"decision": "warning", "detail": {}}),
        "/validation/issues/workflow/summary": _envelope({"missing_scope_count": 0}),
        "/validation/issues/workflow": _envelope(_page([{"bug_id": "bug_1", "workflow_state": "open"}])),
        "/validation/issues/bug_1/workflow": _envelope({"bug_id": "bug_1", "workflow_state": "open"}),
        "/validation/modules/detail-summary": _envelope({"modules": [], "summary": {}}),
        "/validation/pipeline/tests/summary": _envelope({"test_count": 1}),
        "/validation/pipeline/tests": _envelope(_page([{"test_id": "validation_center_backend"}])),
        "/validation/pipeline/tests/validation_center_backend": _envelope({"test_id": "validation_center_backend"}),
        "/validation/features/summary": _envelope({"target_count": 1}),
        "/validation/features": _envelope(_page([{"route_id": "validation.center"}])),
        "/validation/features/validation.center": _envelope({"target": {"route_id": "validation.center"}}),
        "/validation/github/issues/summary": _envelope({"bug_count": 1}),
        "/validation/github/issues": _envelope(_page([{"bug_id": "bug_1", "sync_state": "linked"}])),
        "/validation/git/branches/detail-summary": _envelope({"worktree_count": 1, "branches": [], "worktrees": []}),
        "/validation/github/prs/summary": _envelope({"data_state": "unavailable", "pr_count": 0}),
        "/validation/github/prs": _envelope({"items": [], "total": 0, "page": 1, "page_size": 5, "has_more": False, "data_state": "unavailable"}),
        "/validation/legacy-debt/summary": _envelope({"debt_count": 1}),
        "/validation/legacy-debt/groups": _envelope(_page([{"debt_group_id": "validation:legacy"}])),
        "/validation/legacy-debt/groups/validation%3Alegacy": _envelope({"group": {"debt_group_id": "validation:legacy"}}),
        "/validation/automation/summary": _envelope({"summary": {"gh_authenticated": False}}),
        "/validation/runs": _envelope(_page([{"run_id": "run_1"}])),
        "/validation/runs/run_1": _envelope({"run_id": "run_1", "title": "Run"}),
        "/validation/coverage": _envelope(_page([{"snapshot_id": "cov_1"}])),
        "/validation/coverage/cov_1": _envelope({"summary": {"snapshot_id": "cov_1"}, "snapshot": {"status": "passed"}}),
        "/validation/evidence": _envelope(_page([{"manifest_id": "evidence_1"}])),
        "/validation/evidence/evidence_1": _envelope({"summary": {"manifest_id": "evidence_1"}, "manifest": {"missing_count": 0}}),
        "/validation/executions": _envelope(_page([{"job_id": "valjob_20260504_210000_abcdef12"}])),
        "/validation/executions/valjob_20260504_210000_abcdef12": _envelope({"job_id": "valjob_20260504_210000_abcdef12", "status": "passed"}),
        "/validation/executions/valjob_20260504_210000_abcdef12/log": _envelope({"job_id": "valjob_20260504_210000_abcdef12", "content": "ok\n"}),
        "/validation/executions/valjob_20260504_210000_abcdef12/evidence": _envelope({"job_id": "valjob_20260504_210000_abcdef12", "standard_evidence": {}}),
        "/validation/findings/summary": _envelope({"finding_count": 1}),
        "/validation/findings": _envelope(_page([{"finding_id": "finding_1"}])),
        "/validation/findings/finding_1": _envelope({"finding_id": "finding_1", "agent_context": {"context_type": "quality_finding"}}),
        "/validation/bugs/summary": _envelope({"bug_count": 1}),
        "/validation/bugs": _envelope(_page([{"bug_id": "bug_1"}])),
        "/validation/bugs/bug_1": _envelope({"bug_id": "bug_1", "agent_context": {"context_type": "bug"}}),
        "/validation/bugs/bug_1/agent-context": _envelope({"context_type": "bug", "bug_id": "bug_1"}),
    }
    if path_only not in routes:
        return None, None, f"unexpected path: {path}"
    return routes[path_only]


def test_readonly_smoke_passes_with_complete_contract(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(smoke, "_request_json", lambda _api_base, path, *, timeout: _route(path))
    output = tmp_path / "smoke.json"

    exit_code = smoke.run_smoke(api_base="http://127.0.0.1:8011/api/v1", output=output)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["schema_version"] == smoke.SCHEMA_VERSION
    assert payload["status"] == "passed"
    assert payload["failure_count"] == 0
    assert payload["production_8001_touched"] is False
    assert payload["write_methods_sent"] == []
    assert payload["counts"]["runs"] == 1
    assert payload["counts"]["executions"] == 1
    assert payload["counts"]["bugs"] == 1
    assert payload["endpoint_count"] >= 18


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
    assert "refusing to touch non-localhost validation API" in payload["failures"]
    assert payload["endpoint_count"] == 0


def test_readonly_smoke_fails_on_missing_quality(monkeypatch, tmp_path: Path) -> None:
    def route(path: str):
        if path == "/validation/health":
            return _envelope(
                {
                    "mode": "read_only",
                    "history": {},
                    "plan_catalog": {},
                    "production_8001_touched": False,
                }
            )
        return _route(path)

    monkeypatch.setattr(smoke, "_request_json", lambda _api_base, path, *, timeout: route(path))
    output = tmp_path / "missing_quality.json"

    exit_code = smoke.run_smoke(api_base="http://127.0.0.1:8011/api/v1", output=output)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert "/validation/health quality must be an object" in payload["failures"]
