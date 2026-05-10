"""Tests for scripts/cross_tool_review_dispatch.py (T-PIPE-5.3).

Coverage:
- findings JSON schema validation
- severity filter
- multi-finding apply path with mocked MCP report_bug
- dedup propagation (existing bug returned)
- drawer body composition (new vs deduped vs skipped)
- dry-run does not call mcp_module.report_bug
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DISPATCH_PATH = REPO_ROOT / "scripts" / "cross_tool_review_dispatch.py"


@pytest.fixture
def dispatch_module():
    spec = importlib.util.spec_from_file_location("cross_tool_review_dispatch", DISPATCH_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["cross_tool_review_dispatch"] = module
    spec.loader.exec_module(module)
    yield module
    sys.modules.pop("cross_tool_review_dispatch", None)


@pytest.fixture
def findings_file(tmp_path):
    payload = {
        "schema_version": "aistock_cross_tool_review_findings_v1",
        "reviewer": "claude_code",
        "target_branch": "origin/codex/qe-governance-integration-20260509",
        "commit_list": ["5bce68c", "abcdefg"],
        "findings": [
            {
                "title": "P0 finding alpha",
                "severity": "P0",
                "module": "strategy_package",
                "files": ["backend/services/strategy_package/repository.py"],
                "reproduce_command": "pytest backend/tests/strategy_package/test_repository_service.py -k atomic -q",
                "expected": "atomic transaction",
                "actual": "partial mutation",
                "fix_owner": "codex_app",
                "comments": ["needs Codex review"],
            },
            {
                "title": "P1 finding beta",
                "severity": "P1",
                "module": "qe_archive",
                "files": ["backend/services/qe_archive/handlers/foo.py"],
                "reproduce_command": "pytest backend/tests/qe_archive/test_foo.py",
                "expected": "ok",
                "actual": "fail",
            },
            {
                "title": "P3 finding gamma",
                "severity": "P3",
                "module": "docs.architecture",
                "files": [],
                "reproduce_command": "manual review",
                "expected": "x",
                "actual": "y",
            },
        ],
    }
    p = tmp_path / "findings.json"
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def _mock_mcp_module(*, side_effect=None):
    mcp = MagicMock()
    if side_effect is None:
        # Default: each call returns a fresh, non-deduped bug.
        responses = iter(
            [
                {"deduplicated": False, "bug_id": f"BUG-{i:03d}", "path": f"...{i}", "fingerprint": f"fp{i}"}
                for i in range(100, 200)
            ]
        )
        mcp.report_bug.side_effect = lambda **kw: next(responses)
    else:
        mcp.report_bug.side_effect = side_effect
    return mcp


def test_parse_findings_rejects_wrong_schema(dispatch_module, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"schema_version": "wrong", "findings": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="unsupported"):
        dispatch_module.parse_findings(bad)


def test_parse_findings_requires_findings_list(dispatch_module, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(
        json.dumps({"schema_version": "aistock_cross_tool_review_findings_v1"}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="findings"):
        dispatch_module.parse_findings(bad)


def test_filter_findings_uppercase_match(dispatch_module):
    findings = [
        {"severity": "p0"},  # lowercase OK after upper
        {"severity": "P1"},
        {"severity": "P2"},
    ]
    result = dispatch_module.filter_findings(findings, ["P0", "P1"])
    assert [i for i, _ in result] == [0, 1]


def test_run_dry_run_does_not_invoke_mcp(dispatch_module, findings_file):
    mcp = _mock_mcp_module()
    args = argparse.Namespace(
        findings_json=findings_file,
        target_tool="codex",
        severity_filter="P0,P1",
        related_drawer=None,
        apply=False,
        fix_owner_default=None,
    )
    outcome = dispatch_module.run(args, mcp_module=mcp, drawer_poster=lambda body: None)
    assert outcome["applied"] is False
    assert mcp.report_bug.call_count == 0
    matched = [r for r in outcome["results"] if not r.skipped_reason]
    assert len(matched) == 2  # P0 + P1
    assert all(r.bug_id == "(dry-run)" for r in matched)


def test_run_apply_dispatches_each_finding(dispatch_module, findings_file):
    mcp = _mock_mcp_module()
    args = argparse.Namespace(
        findings_json=findings_file,
        target_tool="codex",
        severity_filter="P0,P1",
        related_drawer="drawer_xyz",
        apply=True,
        fix_owner_default=None,
    )
    outcome = dispatch_module.run(args, mcp_module=mcp, drawer_poster=lambda body: "drawer_999")
    assert outcome["applied"] is True
    assert mcp.report_bug.call_count == 2
    # Each call had related_drawer attached
    for call in mcp.report_bug.call_args_list:
        assert call.kwargs["related_drawer"] == "drawer_xyz"
    matched = [r for r in outcome["results"] if not r.skipped_reason]
    assert sorted(r.bug_id for r in matched) == ["BUG-100", "BUG-101"]
    assert outcome["drawer_id"] == "drawer_999"


def test_run_propagates_dedup_response(dispatch_module, findings_file):
    def dedup_then_new(**kw):
        if kw["title"] == "P0 finding alpha":
            return {
                "deduplicated": True,
                "existing": {"bug_id": "BUG-023", "status": "verified"},
                "fingerprint": "fp_existing",
            }
        return {"deduplicated": False, "bug_id": "BUG-200", "fingerprint": "fp_new"}

    mcp = MagicMock()
    mcp.report_bug.side_effect = dedup_then_new
    args = argparse.Namespace(
        findings_json=findings_file,
        target_tool="codex",
        severity_filter="P0,P1",
        related_drawer=None,
        apply=True,
        fix_owner_default=None,
    )
    outcome = dispatch_module.run(args, mcp_module=mcp, drawer_poster=lambda body: "d-1")
    bug_states = {(r.bug_id, r.deduplicated) for r in outcome["results"] if not r.skipped_reason}
    assert ("BUG-023", True) in bug_states
    assert ("BUG-200", False) in bug_states


def test_compose_drawer_body_includes_sections(dispatch_module):
    R = dispatch_module.FindingResult
    payload = {
        "reviewer": "codex_app",
        "target_branch": "origin/main",
        "commit_list": ["abc1234"],
        "findings": [],
    }
    results = [
        R(0, "alpha", "P0", "qe.archive", "BUG-101", False, "fp1"),
        R(1, "beta", "P1", "paper_v2", "BUG-007", True, "fp2"),
        R(2, "gamma", "P3", "docs", None, False, None, skipped_reason="severity"),
    ]
    body = dispatch_module.compose_drawer_body(payload=payload, target_tool="claude_code", results=results)
    assert "[REVIEW]" in body
    assert "BUG-101" in body
    assert "BUG-007" in body
    assert "Skipped:" in body
    assert "production_touched=no" in body
    assert "branch_reviewed=origin/main" in body


def test_dispatch_one_passes_through_finding_fields(dispatch_module):
    mcp = MagicMock()
    mcp.report_bug.return_value = {"deduplicated": False, "bug_id": "BUG-321", "fingerprint": "fp"}
    finding = {
        "title": "X",
        "severity": "P1",
        "module": "rl_execution",
        "files": ["a.py", "b.py"],
        "reproduce_command": "cmd",
        "expected": "e",
        "actual": "a",
        "fix_owner": "claude_code",
        "comments": ["x", "y"],
    }
    response = dispatch_module.dispatch_one(
        mcp, finding, related_drawer="drawer_abc", fix_owner_default=None
    )
    assert response["bug_id"] == "BUG-321"
    kwargs = mcp.report_bug.call_args.kwargs
    assert kwargs["title"] == "X"
    assert kwargs["files"] == ["a.py", "b.py"]
    assert kwargs["fix_owner"] == "claude_code"
    assert kwargs["related_drawer"] == "drawer_abc"
    assert kwargs["comments"] == ["x", "y"]


def test_dispatch_one_uses_default_fix_owner(dispatch_module):
    mcp = MagicMock()
    mcp.report_bug.return_value = {"deduplicated": False, "bug_id": "BUG-1", "fingerprint": "f"}
    finding = {
        "title": "X",
        "severity": "P2",
        "module": "m",
        "files": [],
        "reproduce_command": "cmd",
    }
    dispatch_module.dispatch_one(mcp, finding, related_drawer=None, fix_owner_default="codex_app")
    assert mcp.report_bug.call_args.kwargs["fix_owner"] == "codex_app"


def test_post_drawer_via_mempalace_returns_none_without_env(dispatch_module, monkeypatch):
    monkeypatch.delenv("MEMPALACE_HTTP_BASE", raising=False)
    assert dispatch_module.post_drawer_via_mempalace("body") is None
