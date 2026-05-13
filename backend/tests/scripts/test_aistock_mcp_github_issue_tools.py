from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


@pytest.fixture()
def mcp_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AISTOCK_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("AISTOCK_VALIDATION_BASE_URL", "http://127.0.0.1/api/v1/validation")
    monkeypatch.setenv("AISTOCK_GITHUB_SKIP_ENV_FILE", "1")
    monkeypatch.setenv("AISTOCK_GITHUB_DISABLE_GH_CLI_TOKEN", "1")
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    sys.modules.pop("scripts.aistock_mcp_server", None)
    module = importlib.import_module("scripts.aistock_mcp_server")
    yield module
    sys.modules.pop("scripts.aistock_mcp_server", None)


def _bugs_dir(tmp_path: Path) -> Path:
    path = tmp_path / "tests" / "aistock_validation" / "bugs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_bug(tmp_path: Path, bug_id: str, **overrides: Any) -> Path:
    payload: dict[str, Any] = {
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": bug_id,
        "title": f"{bug_id} title",
        "description": f"{bug_id} description",
        "module": "qe",
        "severity": "P1",
        "risk_area": "runtime",
        "status": "open",
        "reproduce_command": "pytest backend/tests/scripts/test_aistock_mcp_github_issue_tools.py",
        "evidence_uris": [],
        "fingerprint": f"pytest::{bug_id}",
        "events": [],
    }
    payload.update(overrides)
    path = _bugs_dir(tmp_path) / f"20260513_{bug_id}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def test_github_issue_list_defaults_to_local_registry(mcp_module, tmp_path: Path):
    _write_bug(tmp_path, "BUG-101", title="Open QE bug", module="qe", severity="P1", status="open")
    _write_bug(tmp_path, "BUG-102", title="Fixed UI bug", module="frontend", severity="P2", status="fixed")

    result = mcp_module.mcp_github_issue_list()

    assert result["source"] == "local"
    assert result["registry_is_source_of_truth"] is True
    assert result["total"] == 1
    assert result["items"][0]["bug_id"] == "BUG-101"
    assert result["items"][0]["labels"] == [
        "aistock:bug",
        "module:qe",
        "risk:runtime",
        "severity:p1",
        "status:open",
    ]


def test_github_issue_search_uses_local_registry_without_env(mcp_module, tmp_path: Path):
    _write_bug(
        tmp_path,
        "BUG-201",
        title="Qlib shard failure",
        description="Needle appears in the local source-of-truth record.",
        module="rdagent",
    )
    _write_bug(tmp_path, "BUG-202", title="Other issue", description="No match here.", module="frontend")

    result = mcp_module.mcp_github_issue_search("needle", module="rdagent")

    assert result["source"] == "local"
    assert result["total"] == 1
    assert result["items"][0]["bug_id"] == "BUG-201"


def test_github_issue_create_writes_registry_and_dedupes_without_github_env(mcp_module):
    first = mcp_module.mcp_github_issue_create(
        title="MCP-created issue",
        body="Created through the offline MCP GitHub issue tool.",
        severity="P2",
        module="validation_center",
        labels=["workflow:triage"],
    )

    assert first["deduplicated"] is False
    assert first["github"] == {"created": False, "reason": "create_github_false"}
    written = Path(mcp_module.REPO_ROOT) / first["path"]
    payload = json.loads(written.read_text(encoding="utf-8"))
    assert payload["bug_id"] == first["bug_id"]
    assert payload["description"] == "Created through the offline MCP GitHub issue tool."
    assert payload["custom_github_labels"] == ["workflow:triage"]
    assert "github_issue_number" not in payload

    second = mcp_module.mcp_github_issue_create(
        title="MCP-created issue",
        body="Created through the offline MCP GitHub issue tool.",
        severity="P2",
        module="validation_center",
        labels=["workflow:triage"],
    )

    assert second["deduplicated"] is True
    assert second["existing"]["bug_id"] == first["bug_id"]


def test_github_issue_create_live_requires_explicit_env(mcp_module):
    with pytest.raises(ValueError, match="GH_TOKEN"):
        mcp_module.mcp_github_issue_create(
            title="Needs env",
            body="Live GitHub is opt-in.",
            create_github=True,
        )


def test_github_issue_client_uses_gh_cli_token_fallback(mcp_module, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("AISTOCK_GITHUB_DISABLE_GH_CLI_TOKEN", raising=False)

    def fake_run(args: list[str], **_kwargs: object) -> SimpleNamespace:
        assert args == ["gh", "auth", "token"]
        return SimpleNamespace(returncode=0, stdout="pytest-token\n")

    monkeypatch.setattr(mcp_module.subprocess, "run", fake_run)

    client = mcp_module._github_issue_client_from_env()

    assert client.repo == "owner/repo"
    assert client.token == "pytest-token"


def test_validation_client_ignores_proxy_env_for_loopback(mcp_module, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HTTPS_PROXY", "socks5://127.0.0.1:1080")

    client = mcp_module.ValidationCenterClient(base_url="http://127.0.0.1/api/v1/validation")

    with client._client() as http_client:
        assert http_client._trust_env is False


def test_github_client_skips_unsupported_socks_proxy_env(mcp_module, monkeypatch: pytest.MonkeyPatch):
    original_find_spec = mcp_module.importlib.util.find_spec
    monkeypatch.setenv("HTTPS_PROXY", "socks5://127.0.0.1:1080")

    def fake_find_spec(name: str, *args: object, **kwargs: object) -> object:
        if name == "socksio":
            return None
        return original_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(mcp_module.importlib.util, "find_spec", fake_find_spec)
    client = mcp_module.GitHubIssueClient(repo="owner/repo", token="pytest-token")

    with client._client() as http_client:
        assert http_client._trust_env is False


def test_github_issue_create_live_path_is_mockable(mcp_module, monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    class FakeGitHubClient:
        def __init__(self, *, repo: str, token: str) -> None:
            captured["repo"] = repo
            captured["token"] = token

        def create_issue(self, *, title: str, body: str, labels: list[str]) -> dict[str, Any]:
            captured["title"] = title
            captured["body"] = body
            captured["labels"] = labels
            return {
                "number": 77,
                "state": "open",
                "html_url": "https://github.example/issues/77",
            }

    monkeypatch.setenv("GH_TOKEN", "pytest-token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setattr(mcp_module, "_github_client_factory", FakeGitHubClient)

    result = mcp_module.mcp_github_issue_create(
        title="Mirror me",
        body="This should be mirrored after registry record assembly.",
        severity="P1",
        module="mcp",
        labels=["workflow:triage"],
        create_github=True,
    )

    assert captured["repo"] == "owner/repo"
    assert captured["token"] == "pytest-token"
    assert captured["title"].startswith("[BUG-001] Mirror me")
    assert "<!-- aistock-bug-id: BUG-001 -->" in captured["body"]
    assert "aistock:bug" in captured["labels"]
    assert "workflow:triage" in captured["labels"]
    assert result["github"]["number"] == 77
    payload = json.loads((Path(mcp_module.REPO_ROOT) / result["path"]).read_text(encoding="utf-8"))
    assert payload["github_issue_number"] == 77
    assert payload["github_issue_url"] == "https://github.example/issues/77"


def test_github_issue_list_both_merges_live_mirror(mcp_module, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _write_bug(tmp_path, "BUG-301", title="Registry issue", module="mcp", severity="P1")

    class FakeGitHubClient:
        def __init__(self, *, repo: str, token: str) -> None:
            self.repo = repo
            self.token = token

        def list_issues(self, *, state: str = "open", labels: list[str] | None = None) -> list[dict[str, Any]]:
            assert state == "all"
            return [
                {
                    "source": "github",
                    "registry_is_source_of_truth": False,
                    "bug_id": "BUG-301",
                    "number": 88,
                    "title": "[BUG-301] Registry issue",
                    "body": "<!-- aistock-bug-id: BUG-301 -->",
                    "state": "open",
                    "status": "open",
                    "severity": "P1",
                    "module": "mcp",
                    "labels": ["aistock:bug", "severity:p1", "module:mcp"],
                    "html_url": "https://github.example/issues/88",
                }
            ]

    monkeypatch.setenv("GH_TOKEN", "pytest-token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setattr(mcp_module, "_github_client_factory", FakeGitHubClient)

    result = mcp_module.mcp_github_issue_list(source="both", state="all")

    assert result["total"] == 1
    item = result["items"][0]
    assert item["source"] == "bug_json"
    assert item["registry_is_source_of_truth"] is True
    assert item["github_issue"]["number"] == 88
    assert item["html_url"] == "https://github.example/issues/88"
