from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


class StubFastMCP:
    def __init__(self, _name: str) -> None:
        self.tools: dict[str, object] = {}

    def tool(self, name: str | None = None, **_kwargs):
        def decorator(func):
            self.tools[name or func.__name__] = func
            return func

        return decorator

    def run(self, **_kwargs) -> None:
        return None


def _install_stub_fastmcp() -> None:
    """Force tests to import the MCP server without the external runtime."""
    mcp_module = types.ModuleType("mcp")
    server_module = types.ModuleType("mcp.server")
    fastmcp_module = types.ModuleType("mcp.server.fastmcp")
    fastmcp_module.FastMCP = StubFastMCP
    mcp_module.server = server_module
    server_module.fastmcp = fastmcp_module
    sys.modules["mcp"] = mcp_module
    sys.modules["mcp.server"] = server_module
    sys.modules["mcp.server.fastmcp"] = fastmcp_module


@pytest.fixture()
def mcp_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AISTOCK_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("AISTOCK_VALIDATION_BASE_URL", "http://127.0.0.1/api/v1/validation")
    monkeypatch.setenv("AISTOCK_GITHUB_SKIP_ENV_FILE", "1")
    monkeypatch.setenv("AISTOCK_GITHUB_DISABLE_GH_CLI_TOKEN", "1")
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    _install_stub_fastmcp()
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


def _write_bug_with_encoding(tmp_path: Path, bug_id: str, *, encoding: str, **overrides: Any) -> Path:
    path = _write_bug(tmp_path, bug_id, **overrides)
    payload = json.loads(path.read_text(encoding="utf-8"))
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding=encoding)
    return path


def test_github_issue_list_defaults_to_local_registry(mcp_module, tmp_path: Path):
    _write_bug(tmp_path, "BUG-101", title="Open QE bug", module="qe", severity="P1", status="open")
    _write_bug(tmp_path, "BUG-102", title="Fixed UI bug", module="frontend", severity="P2", status="fixed")

    result = mcp_module.mcp_github_issue_list()

    assert result["source"] == "local"
    assert result["registry_is_source_of_truth"] is True
    assert result["compact"] is True
    assert result["total"] == 1
    assert result["items"][0]["bug_id"] == "BUG-101"
    assert "body" not in result["items"][0]
    assert "reproduce_command" not in result["items"][0]
    assert result["items"][0]["labels"] == [
        "aistock:bug",
        "module:qe",
        "P1",
        "risk:runtime",
        "severity:p1",
        "status:open",
    ]


def test_github_issue_list_full_mode_preserves_body(mcp_module, tmp_path: Path):
    _write_bug(tmp_path, "BUG-101", title="Open QE bug", module="qe", status="open")

    result = mcp_module.mcp_github_issue_list(compact=False)

    assert result["compact"] is False
    assert result["items"][0]["body"]
    assert result["items"][0]["reproduce_command"]


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
    assert result["compact"] is True
    assert result["total"] == 1
    assert result["items"][0]["bug_id"] == "BUG-201"


def test_github_issue_search_tolerates_bom_encoded_registry_file(mcp_module, tmp_path: Path):
    _write_bug_with_encoding(
        tmp_path,
        "BUG-211",
        encoding="utf-8-sig",
        title="BOM encoded registry issue",
        description="Needle appears in a BOM encoded local source-of-truth record.",
        module="validation_center",
    )
    _write_bug(tmp_path, "BUG-212", title="Other issue", description="No match here.", module="frontend")

    result = mcp_module.mcp_github_issue_search("needle", module="validation_center")

    assert result["source"] == "local"
    assert result["total"] == 1
    assert result["items"][0]["bug_id"] == "BUG-211"


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


def test_github_issue_client_infers_repo_from_git_remote(
    mcp_module,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.setenv("GH_TOKEN", "pytest-token")

    def fake_run(args: list[str], **_kwargs: object) -> SimpleNamespace:
        if args[:3] == ["git", "-C", str(tmp_path)] and args[3:] == ["rev-parse", "--show-toplevel"]:
            return SimpleNamespace(returncode=0, stdout=f"{tmp_path}\n")
        if args[:3] == ["git", "-C", str(tmp_path)] and args[3:] == ["remote", "get-url", "origin"]:
            return SimpleNamespace(returncode=0, stdout="git@github.com:owner/repo.git\n")
        return SimpleNamespace(returncode=1, stdout="", stderr="not a test repo")

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
    assert "<!-- aistock-registry-path: tests/aistock_validation/bugs/" in captured["body"]
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
    assert set(item["github_issue"]) == {"number", "state", "title", "html_url"}
    assert item["html_url"] == "https://github.example/issues/88"


def test_assign_bug_updates_registry_status_and_owner(mcp_module, tmp_path: Path):
    path = _write_bug(tmp_path, "BUG-401", status="open", assigned_agent=None, fix_branch=None)

    result = mcp_module.assign_bug(
        "BUG-401",
        assigned_agent="codex_app",
        fix_branch="codex/fix-bug-401",
        actor="pytest",
        note="claiming for test",
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert result["current"]["status"] == "in_progress"
    assert payload["status"] == "in_progress"
    assert payload["assigned_agent"] == "codex_app"
    assert payload["fix_branch"] == "codex/fix-bug-401"
    assert payload["events"][-1]["action"] == "assigned"
    assert result["github"] == {"synced": False, "reason": "sync_github_false"}


def test_update_bug_status_records_fix_and_verification_fields(mcp_module, tmp_path: Path):
    path = _write_bug(tmp_path, "BUG-402", status="in_progress")

    fixed = mcp_module.update_bug_status(
        "BUG-402",
        "fixed",
        actor="pytest",
        fix_commit="abc1234",
        note="fixed in test",
    )
    verified = mcp_module.update_bug_status(
        "BUG-402",
        "verified",
        actor="pytest-reviewer",
        verification_run_id="run-402",
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert fixed["current"]["status"] == "fixed"
    assert payload["status"] == "verified"
    assert payload["fix_commit"] == "abc1234"
    assert payload["verification_run_id"] == "run-402"
    assert payload["fixed_at"]
    assert payload["closed_at"]
    assert [event["action"] for event in payload["events"][-2:]] == ["status_changed", "status_changed"]
    assert verified["current"]["verification_run_id"] == "run-402"


def test_sync_bug_json_to_github_creates_issue_and_backfills_link(
    mcp_module,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    path = _write_bug(tmp_path, "BUG-403", title="Sync create", module="mcp", severity="P1")
    captured: dict[str, Any] = {}

    class FakeGitHubClient:
        def __init__(self, *, repo: str, token: str) -> None:
            captured["repo"] = repo
            captured["token"] = token

        def list_issues(self, *, state: str = "open", labels: list[str] | None = None) -> list[dict[str, Any]]:
            captured["list"] = {"state": state, "labels": labels}
            return []

        def create_issue(self, *, title: str, body: str, labels: list[str]) -> dict[str, Any]:
            captured["created"] = {"title": title, "body": body, "labels": labels}
            return {
                "number": 403,
                "title": title,
                "body": body,
                "state": "open",
                "labels": [{"name": label} for label in labels],
                "html_url": "https://github.example/issues/403",
            }

        def update_issue(self, number: int, changes: dict[str, Any]) -> dict[str, Any]:
            raise AssertionError("open issue creation should not need a follow-up update")

    monkeypatch.setenv("GH_TOKEN", "pytest-token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setattr(mcp_module, "_github_client_factory", FakeGitHubClient)

    result = mcp_module.mcp_github_issue_sync_bug("BUG-403", apply=True, actor="pytest")

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert result["results"][0]["action"] == "create"
    assert captured["repo"] == "owner/repo"
    assert captured["created"]["title"] == "[BUG-403] Sync create"
    assert "<!-- aistock-bug-id: BUG-403 -->" in captured["created"]["body"]
    assert "status:open" in captured["created"]["labels"]
    assert payload["github_issue_number"] == 403
    assert payload["github_issue_url"] == "https://github.example/issues/403"
    assert payload["events"][-1]["action"] == "github_issue_created"


def test_sync_bug_tolerates_unrelated_bom_encoded_registry_file(
    mcp_module,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _write_bug_with_encoding(
        tmp_path,
        "BUG-405",
        encoding="utf-8-sig",
        title="Unrelated BOM issue",
        module="mcp",
    )
    path = _write_bug(tmp_path, "BUG-406", title="Target sync issue", module="mcp", severity="P1")
    captured: dict[str, Any] = {}

    class FakeGitHubClient:
        def __init__(self, *, repo: str, token: str) -> None:
            captured["repo"] = repo
            captured["token"] = token

        def list_issues(self, *, state: str = "open", labels: list[str] | None = None) -> list[dict[str, Any]]:
            captured["list"] = {"state": state, "labels": labels}
            return []

        def create_issue(self, *, title: str, body: str, labels: list[str]) -> dict[str, Any]:
            captured["created"] = {"title": title, "body": body, "labels": labels}
            return {
                "number": 406,
                "title": title,
                "body": body,
                "state": "open",
                "labels": [{"name": label} for label in labels],
                "html_url": "https://github.example/issues/406",
            }

        def update_issue(self, number: int, changes: dict[str, Any]) -> dict[str, Any]:
            raise AssertionError("open issue creation should not need a follow-up update")

    monkeypatch.setenv("GH_TOKEN", "pytest-token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setattr(mcp_module, "_github_client_factory", FakeGitHubClient)

    result = mcp_module.mcp_github_issue_sync_bug("BUG-406", apply=True, actor="pytest")

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert result["results"][0]["action"] == "create"
    assert captured["created"]["title"] == "[BUG-406] Target sync issue"
    assert payload["github_issue_number"] == 406
    assert payload["github_issue_url"] == "https://github.example/issues/406"


def test_sync_github_to_json_backfills_status_label_and_link(
    mcp_module,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    path = _write_bug(tmp_path, "BUG-404", status="open")

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
                    "bug_id": "BUG-404",
                    "number": 404,
                    "title": "[BUG-404] Remote verified",
                    "body": "<!-- aistock-bug-id: BUG-404 -->",
                    "state": "closed",
                    "status": "verified",
                    "severity": "P1",
                    "module": "mcp",
                    "labels": [
                        "aistock:bug",
                        "severity:p1",
                        "module:mcp",
                        "status:verified",
                    ],
                    "html_url": "https://github.example/issues/404",
                }
            ]

    monkeypatch.setenv("GH_TOKEN", "pytest-token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setattr(mcp_module, "_github_client_factory", FakeGitHubClient)

    result = mcp_module.mcp_github_issue_sync_bug(
        "BUG-404",
        direction="github-to-json",
        apply=True,
        actor="pytest",
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert result["results"][0]["action"] == "update_json"
    assert payload["github_issue_number"] == 404
    assert payload["github_issue_url"] == "https://github.example/issues/404"
    assert payload["status"] == "verified"
    assert payload["closed_at"]
    assert payload["events"][-1]["action"] == "github_issue_synced_to_json"


# ---------------------------------------------------------------------------
# BUG-102 regression: parse errors on individual BUG JSON files must not
# abort the entire registry scan.  A single corrupted file must be skipped
# with a warning; unrelated files must still be readable.
# ---------------------------------------------------------------------------


def _write_corrupted_json(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not valid json\n", encoding="utf-8")


def test_github_issue_list_skips_corrupted_registry_file(mcp_module, tmp_path: Path):
    _write_bug(tmp_path, "BUG-301", title="Good bug", module="qe")
    _write_corrupted_json(_bugs_dir(tmp_path) / "20260520_BUG-CORRUPT.json")

    result = mcp_module.mcp_github_issue_list()

    assert result["source"] == "local"
    bug_ids = {item["bug_id"] for item in result["items"] if item.get("bug_id")}
    assert "BUG-301" in bug_ids
    assert "BUG-CORRUPT" not in bug_ids


def test_github_issue_search_skips_corrupted_registry_file(mcp_module, tmp_path: Path):
    _write_bug(tmp_path, "BUG-302", title="search target", description="findme", module="qe")
    _write_corrupted_json(_bugs_dir(tmp_path) / "20260520_BUG-CORRUPT.json")

    result = mcp_module.mcp_github_issue_search("findme", module="qe")

    assert result["total"] == 1
    assert result["items"][0]["bug_id"] == "BUG-302"


def test_github_issue_sync_tolerates_corrupted_unrelated_file(
    mcp_module, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    _write_bug(tmp_path, "BUG-303", status="open", title="sync me", github_issue_number=None)
    _write_corrupted_json(_bugs_dir(tmp_path) / "20260520_BUG-CORRUPT.json")

    class FakeGitHubClient:
        def __init__(self, *, repo: str, token: str, **__: Any) -> None:
            pass

        def list_issues(self, *, state: str = "open", labels: list[str] | None = None) -> list[dict[str, Any]]:
            return [{
                "number": 303,
                "title": "[BUG-303] synced",
                "body": "<!-- aistock-bug-id: BUG-303 -->\nSynced content",
                "state": "closed",
                "status": "verified",
                "module": "mcp",
                "severity": "P1",
                "labels": [
                    "aistock:bug", "severity:p1", "module:mcp", "status:verified",
                ],
                "html_url": "https://github.example/issues/303",
            }]

    monkeypatch.setenv("GH_TOKEN", "pytest-token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setattr(mcp_module, "_github_client_factory", FakeGitHubClient)

    result = mcp_module.mcp_github_issue_sync_bug(
        "BUG-303", direction="github-to-json", apply=True, actor="pytest",
    )
    # The corrupted file must not prevent the sync from finding BUG-303.
    # The result action depends on whether status/link changes are detected;
    # the invariant is that the call succeeds without crashing.
    assert isinstance(result, dict)
    assert "results" in result
