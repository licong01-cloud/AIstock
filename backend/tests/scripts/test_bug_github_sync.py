from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.bug_github_sync as sync


def _write_bug(tmp_path: Path, *, bug_id: str = "BUG-900", severity: str = "P1", status: str = "open") -> dict[str, object]:
    bug = {
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": bug_id,
        "title": "Synthetic validation failure",
        "description": "A reproducible failure from the validation catalog.",
        "module": "validation_center",
        "severity": severity,
        "risk_area": "data_correctness",
        "status": status,
        "fingerprint": f"pytest::{bug_id}",
        "reproduce_command": "pytest backend/tests/scripts/test_bug_github_sync.py",
        "evidence_uris": ["memory:synthetic"],
        "required_verification": ["offline unit test passes"],
        "closure_requirements": ["issue mirror remains idempotent"],
    }
    path = tmp_path / f"20260512_{bug_id}.json"
    path.write_text(json.dumps(bug), encoding="utf-8")
    return bug


def test_maps_bug_json_to_github_issue_payload(tmp_path: Path) -> None:
    bug = _write_bug(tmp_path, bug_id="BUG-901", severity="P1")
    bug["_source_path"] = str(tmp_path / "20260512_BUG-901.json")

    plan = sync.plan_json_to_issues([bug], historical_import=True)

    assert plan[0]["action"] == "create"
    desired = plan[0]["desired"]
    assert desired["title"] == "[BUG-901] Synthetic validation failure"
    assert "<!-- aistock-bug-id: BUG-901 -->" in desired["body"]
    assert "bugs JSON entry remains the source of truth" in desired["body"]
    assert desired["state"] == "open"
    assert set(desired["labels"]) == {
        "P1",
        "aistock:bug",
        "import:historical",
        "module:validation_center",
        "risk:data_correctness",
        "severity:p1",
        "status:open",
    }


def test_existing_issue_snapshot_makes_planning_idempotent(tmp_path: Path) -> None:
    bug = _write_bug(tmp_path, bug_id="BUG-902", severity="P0")
    bug["_source_path"] = str(tmp_path / "20260512_BUG-902.json")
    create_plan = sync.plan_json_to_issues([bug], historical_import=True)
    desired = create_plan[0]["desired"]
    existing = {
        "number": 42,
        "title": desired["title"],
        "body": desired["body"],
        "state": desired["state"],
        "labels": [{"name": label} for label in desired["labels"]],
        "html_url": "https://github.example/issues/42",
    }

    plan = sync.plan_json_to_issues([bug], [sync.normalize_issue(existing)], historical_import=True)

    assert plan[0]["action"] == "noop"
    assert plan[0]["issue_number"] == 42
    assert plan[0]["changes"] == {}


def test_existing_issue_drift_plans_update_without_network(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bug = _write_bug(tmp_path, bug_id="BUG-903", severity="P1")
    bug["_source_path"] = str(tmp_path / "20260512_BUG-903.json")
    monkeypatch.setattr(sync.request, "urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("network used")))

    existing = sync.normalize_issue({
        "number": 7,
        "title": "[BUG-903] stale title",
        "body": "<!-- aistock-bug-id: BUG-903 -->\nstale body",
        "state": "open",
        "labels": ["aistock:bug", "severity:p1"],
    })
    plan = sync.plan_json_to_issues([bug], [existing], historical_import=True)

    assert plan[0]["action"] == "update"
    assert plan[0]["changes"]["title"] == "[BUG-903] Synthetic validation failure"
    assert "import:historical" in plan[0]["changes"]["labels"]


def test_p0_p1_filter_skips_lower_severity_imports(tmp_path: Path) -> None:
    p1_bug = _write_bug(tmp_path, bug_id="BUG-904", severity="P1")
    p2_bug = _write_bug(tmp_path, bug_id="BUG-905", severity="P2")
    p1_bug["_source_path"] = "p1.json"
    p2_bug["_source_path"] = "p2.json"

    plan = sync.plan_json_to_issues([p1_bug, p2_bug], historical_import=True, p0_p1_only=True)

    assert [item["action"] for item in plan] == ["create", "skip"]
    assert plan[1]["reason"] == "severity_filter_p0_p1_only"


def test_cli_defaults_to_offline_dry_run_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _write_bug(tmp_path, bug_id="BUG-906", severity="P1")
    monkeypatch.setattr(sync.request, "urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("network used")))

    assert sync.main(["--bugs-dir", str(tmp_path), "--historical-import", "--p0-p1-only", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "planned"
    assert payload["dry_run"] is True
    assert payload["historical_import"] is True
    assert payload["p0_p1_only"] is True
    assert payload["summary"] == {"create": 1}
    assert payload["plan"][0]["desired"]["labels"].count("import:historical") == 1


def test_cli_apply_requires_token_before_any_network(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _write_bug(tmp_path, bug_id="BUG-907", severity="P1")
    network_called = False

    def fail_network(*_args: object, **_kwargs: object) -> object:
        nonlocal network_called
        network_called = True
        raise AssertionError("network used before token guard")

    monkeypatch.setattr(sync.request, "urlopen", fail_network)

    code = sync.main(["--bugs-dir", str(tmp_path), "--apply", "--repo", "owner/repo", "--token", ""])

    assert code == 2
    assert network_called is False
    assert "--apply requires --token" in capsys.readouterr().err


def test_run_uses_offline_issue_snapshot_for_noop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bug = _write_bug(tmp_path, bug_id="BUG-908", severity="P1")
    loaded = sync.load_bug_files(tmp_path)[0]
    desired = sync.plan_json_to_issues([loaded], historical_import=True)[0]["desired"]
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    snapshot = snapshot_dir / "issues.json"
    snapshot.write_text(json.dumps({"issues": [{"number": 8, **desired}]}), encoding="utf-8")
    monkeypatch.setattr(sync.request, "urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("network used")))

    result = sync.run(sync.SyncConfig(bugs_dir=tmp_path, historical_import=True, issues_snapshot=snapshot))

    assert result["dry_run"] is True
    assert result["summary"] == {"noop": 1}
    assert result["plan"][0]["bug_id"] == bug["bug_id"]


def test_load_bug_files_accepts_utf8_bom(tmp_path: Path) -> None:
    bug = _write_bug(tmp_path, bug_id="BUG-922", severity="P1")
    target = tmp_path / "20260512_BUG-922.json"
    target.write_text(json.dumps(bug), encoding="utf-8-sig")

    loaded = sync.load_bug_files(tmp_path)

    assert loaded[0]["bug_id"] == "BUG-922"


def test_verified_bug_maps_to_closed_issue_state(tmp_path: Path) -> None:
    bug = _write_bug(tmp_path, bug_id="BUG-909", severity="P1", status="verified")
    bug["_source_path"] = "bug.json"

    plan = sync.plan_json_to_issues([bug])

    assert plan[0]["desired"]["state"] == "closed"


def test_issue_bug_id_falls_back_to_title_prefix() -> None:
    issue = sync.normalize_issue({"number": 91, "title": "[BUG-910] title only", "body": ""})

    assert sync.issue_bug_id(issue) == "BUG-910"


def test_issue_bug_id_handles_marker_variants_and_body_field() -> None:
    marker_issue = sync.normalize_issue({
        "number": 90,
        "title": "missing title id",
        "body": "<!-- AISTOCK_BUG_ID = bug-prep-001 -->",
    })
    field_issue = sync.normalize_issue({"number": 91, "title": "missing", "body": '"bug_id": "BUG-911",'})

    assert sync.issue_bug_id(marker_issue) == "BUG-PREP-001"
    assert sync.issue_bug_id(field_issue) == "BUG-911"


def test_conflicting_issue_markers_are_skipped_for_safety(tmp_path: Path) -> None:
    issue = {
        "number": 912,
        "title": "[BUG-912] conflict",
        "body": "<!-- aistock-bug-id: BUG-912 --><!-- aistock-bug-id: BUG-913 -->",
        "labels": ["severity:p1"],
    }

    plan = sync.plan_issues_to_json([issue], [], bugs_dir=tmp_path)

    assert plan[0]["action"] == "skip"
    assert plan[0]["reason"] == "conflicting_issue_markers"
    assert plan[0]["bug_ids"] == ["BUG-912", "BUG-913"]


def test_load_issues_snapshot_accepts_plain_list(tmp_path: Path) -> None:
    snapshot = tmp_path / "issues.json"
    snapshot.write_text(json.dumps([{"number": 11, "title": "[BUG-911] ok", "labels": ["P1"]}]), encoding="utf-8")

    issues = sync.load_issues_snapshot(snapshot)

    assert issues[0]["number"] == 11
    assert issues[0]["labels"] == ["P1"]


def test_load_issues_snapshot_accepts_utf8_bom(tmp_path: Path) -> None:
    snapshot = tmp_path / "issues.json"
    snapshot.write_text(json.dumps([{"number": 12, "title": "[BUG-912] ok", "labels": ["P1"]}]), encoding="utf-8-sig")

    issues = sync.load_issues_snapshot(snapshot)

    assert issues[0]["number"] == 12


def test_issue_severity_prefers_labels_then_title() -> None:
    assert sync.issue_severity({"labels": ["severity:p0"], "title": "[P2] stale"}) == "P0"
    assert sync.issue_severity({"labels": [], "title": "[P1] release blocker"}) == "P1"
    assert sync.issue_severity({"labels": [], "title": "plain"}) == "P2"


def test_issue_module_uses_module_label() -> None:
    issue = {"labels": ["bug", "module:validation.center"], "body": "Module: wrong"}

    assert sync.issue_module(issue) == "validation.center"


def test_normalize_issue_accepts_gh_cli_url_alias() -> None:
    issue = sync.normalize_issue({"number": 911, "title": "ok", "url": "https://github.example/issues/911"})

    assert issue["html_url"] == "https://github.example/issues/911"


def test_plan_issues_to_json_creates_missing_bug_entry(tmp_path: Path) -> None:
    issue = {
        "number": 912,
        "title": "[P1] Imported failure",
        "body": "A GitHub-only issue.",
        "state": "open",
        "labels": ["severity:p1", "module:paper_v2"],
        "html_url": "https://github.example/issues/912",
    }

    plan = sync.plan_issues_to_json([issue], [], bugs_dir=tmp_path, p0_p1_only=True)

    assert plan[0]["action"] == "create_json"
    assert plan[0]["bug_id"] == "BUG-GH-912"
    assert plan[0]["desired"]["module"] == "paper_v2"
    assert plan[0]["desired"]["severity"] == "P1"
    assert str(tmp_path) in plan[0]["path"]


def test_plan_issues_to_json_skips_lower_severity_issue_when_filtered(tmp_path: Path) -> None:
    issue = {"number": 913, "title": "[P2] Low priority", "state": "open", "labels": ["severity:p2"]}

    plan = sync.plan_issues_to_json([issue], [], bugs_dir=tmp_path, p0_p1_only=True)

    assert plan[0]["action"] == "skip"
    assert plan[0]["reason"] == "severity_filter_p0_p1_only"


def test_plan_issues_to_json_updates_closed_status_for_existing_bug(tmp_path: Path) -> None:
    bug = _write_bug(tmp_path, bug_id="BUG-914", severity="P1", status="open")
    loaded = sync.load_bug_files(tmp_path)[0]
    issue = {
        "number": 914,
        "title": "[BUG-914] Synthetic validation failure",
        "body": "<!-- aistock-bug-id: BUG-914 -->",
        "state": "closed",
        "labels": ["severity:p1"],
        "html_url": "https://github.example/issues/914",
    }

    plan = sync.plan_issues_to_json([issue], [loaded], bugs_dir=tmp_path)

    assert plan[0]["action"] == "update_json"
    assert plan[0]["changes"]["status"] == "closed"
    assert plan[0]["changes"]["github_issue_number"] == 914
    assert plan[0]["bug_id"] == bug["bug_id"]


def test_closed_issue_status_prefers_verified_status_label(tmp_path: Path) -> None:
    loaded = sync.load_bug_files(tmp_path) if tmp_path.exists() else []
    issue = {
        "number": 920,
        "title": "[BUG-920] Verified upstream",
        "body": "<!-- aistock-bug-id: BUG-920 -->",
        "state": "closed",
        "labels": ["severity:p1", "status:verified"],
        "html_url": "https://github.example/issues/920",
    }

    plan = sync.plan_issues_to_json([issue], loaded, bugs_dir=tmp_path)

    assert plan[0]["action"] == "create_json"
    assert plan[0]["desired"]["status"] == "verified"
    assert plan[0]["desired"]["closed_at"] is not None


def test_apply_issues_to_json_plan_creates_file(tmp_path: Path) -> None:
    issue = {
        "number": 915,
        "title": "[P1] Create me",
        "body": "new bug",
        "state": "open",
        "labels": ["severity:p1"],
    }
    plan = sync.plan_issues_to_json([issue], [], bugs_dir=tmp_path)

    results = sync.apply_issues_to_json_plan(plan)

    assert results[0]["action"] == "created_json"
    created_path = Path(results[0]["path"])
    assert created_path.exists()
    payload = json.loads(created_path.read_text(encoding="utf-8"))
    assert payload["bug_id"] == "BUG-GH-915"


def test_apply_issues_to_json_plan_records_status_event(tmp_path: Path) -> None:
    _write_bug(tmp_path, bug_id="BUG-916", severity="P1", status="open")
    loaded = sync.load_bug_files(tmp_path)[0]
    issue = {
        "number": 916,
        "title": "[BUG-916] Close me",
        "body": "<!-- aistock-bug-id: BUG-916 -->",
        "state": "closed",
        "labels": ["severity:p1"],
    }
    plan = sync.plan_issues_to_json([issue], [loaded], bugs_dir=tmp_path)

    results = sync.apply_issues_to_json_plan(plan)

    payload = json.loads(Path(results[0]["path"]).read_text(encoding="utf-8"))
    assert payload["status"] == "closed"
    assert payload["closed_at"] is not None
    assert payload["events"][-1]["action"] == "status_synced_from_github_issue"


def test_apply_issues_to_json_plan_blocks_canonical_root_main(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_bug(tmp_path, bug_id="BUG-916", severity="P1", status="open")
    target = tmp_path / "20260512_BUG-916.json"
    plan = [
        {
            "action": "update_json",
            "bug_id": "BUG-916",
            "issue_number": 916,
            "path": str(target),
            "changes": {"status": "verified"},
        }
    ]
    monkeypatch.setattr(sync, "_git_toplevel", lambda _path: tmp_path)
    monkeypatch.setattr(sync, "_canonical_root", lambda: tmp_path)
    monkeypatch.setattr(sync, "_git_branch", lambda _root: "main")

    with pytest.raises(sync.BugGitHubSyncError, match="canonical root main"):
        sync.apply_issues_to_json_plan(plan)

    assert json.loads(target.read_text(encoding="utf-8"))["status"] == "open"


def test_apply_issues_to_json_plan_refuses_overwrite(tmp_path: Path) -> None:
    target = tmp_path / "existing.json"
    target.write_text("{}", encoding="utf-8")
    plan = [{"action": "create_json", "bug_id": "BUG-GH-916", "issue_number": 916, "path": str(target), "desired": {}}]

    with pytest.raises(sync.BugGitHubSyncError, match="refusing to overwrite"):
        sync.apply_issues_to_json_plan(plan)


def test_apply_plan_uses_fake_client_without_network() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.created: list[dict[str, object]] = []

        def create_issue(self, desired: dict[str, object]) -> dict[str, object]:
            self.created.append(desired)
            return {"number": 99, "html_url": "https://github.example/issues/99"}

        def update_issue(self, number: int, changes: dict[str, object]) -> dict[str, object]:
            raise AssertionError("unexpected update")

    client = FakeClient()
    results = sync.apply_plan([{"bug_id": "BUG-917", "action": "create", "desired": {"title": "t", "body": "b", "labels": []}}], client)  # type: ignore[arg-type]

    assert results == [{"bug_id": "BUG-917", "action": "created", "issue_number": 99, "issue_url": "https://github.example/issues/99"}]
    assert client.created[0]["title"] == "t"


def test_cli_all_severities_overrides_p0_p1_filter(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write_bug(tmp_path, bug_id="BUG-918", severity="P2")

    assert sync.main(["--bugs-dir", str(tmp_path), "--p0-p1-only", "--all-severities", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["p0_p1_only"] is False
    assert payload["summary"] == {"create": 1}


def test_cli_bidirectional_dry_run_uses_issue_snapshot(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write_bug(tmp_path, bug_id="BUG-919", severity="P1")
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    snapshot = snapshot_dir / "issues.json"
    snapshot.write_text(
        json.dumps({"issues": [{"number": 920, "title": "[P1] GitHub only", "state": "open", "labels": ["severity:p1"]}]}),
        encoding="utf-8",
    )

    assert sync.main(["--bugs-dir", str(tmp_path), "--issues-snapshot", str(snapshot), "--direction", "both", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["direction"] == "both"
    assert payload["json_to_issues_summary"] == {"create": 1}
    assert payload["issues_to_json_summary"] == {"create_json": 1}


def test_cli_apply_json_to_issues_requires_token_even_with_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_bug(tmp_path, bug_id="BUG-921", severity="P1")
    snapshot = tmp_path / "issues.json"
    snapshot.write_text(json.dumps({"issues": []}), encoding="utf-8")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.setenv("AISTOCK_GITHUB_DISABLE_GH_CLI_TOKEN", "1")

    code = sync.main(["--bugs-dir", str(tmp_path), "--issues-snapshot", str(snapshot), "--apply", "--repo", "owner/repo"])

    assert code == 2
    assert "--apply requires --token" in capsys.readouterr().err


def test_local_env_loader_sets_github_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env.github-issues-local"
    env_file.write_text(
        "GITHUB_REPOSITORY=owner/repo\n"
        "HTTPS_PROXY=socks5://127.0.0.1:1080\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.delenv("HTTPS_PROXY", raising=False)

    sync._load_local_github_env()

    assert sync.os.environ["GITHUB_REPOSITORY"] == "owner/repo"
    assert sync.os.environ["HTTPS_PROXY"] == "socks5://127.0.0.1:1080"


def test_run_infers_repo_from_git_remote_for_live_sync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_bug(tmp_path, bug_id="BUG-922", severity="P1")
    captured: dict[str, object] = {}
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.setenv("AISTOCK_GITHUB_SKIP_ENV_FILE", "1")

    def fake_run(args: list[str], **_kwargs: object) -> SimpleNamespace:
        if args[:3] == ["git", "-C", str(tmp_path)] and args[3:] == ["rev-parse", "--show-toplevel"]:
            return SimpleNamespace(returncode=0, stdout=f"{tmp_path}\n")
        if args[:3] == ["git", "-C", str(tmp_path)] and args[3:] == ["remote", "get-url", "origin"]:
            return SimpleNamespace(returncode=0, stdout="https://github.com/owner/repo.git\n")
        return SimpleNamespace(returncode=1, stdout="", stderr="not a test repo")

    class FakeGitHubClient:
        def __init__(self, *, repo: str, token: str) -> None:
            captured["repo"] = repo
            captured["token"] = token

        def list_issues(self) -> list[dict[str, object]]:
            return []

        def create_issue(self, desired: dict[str, object]) -> dict[str, object]:
            captured["created_title"] = desired["title"]
            return {"number": 922, "html_url": "https://github.example/issues/922"}

        def update_issue(self, number: int, changes: dict[str, object]) -> dict[str, object]:
            raise AssertionError("unexpected update")

    monkeypatch.setattr(sync.subprocess, "run", fake_run)
    monkeypatch.setattr(sync, "GitHubClient", FakeGitHubClient)

    payload = sync.run(sync.SyncConfig(bugs_dir=tmp_path, apply=True, token="pytest-token"))

    assert payload["repo"] == "owner/repo"
    assert captured["repo"] == "owner/repo"
    assert captured["token"] == "pytest-token"
    assert captured["created_title"] == "[BUG-922] Synthetic validation failure"


def test_github_token_default_uses_gh_cli_only_when_remote_needed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("AISTOCK_GITHUB_DISABLE_GH_CLI_TOKEN", raising=False)
    calls: list[list[str]] = []

    def fake_run(args: list[str], **_kwargs: object) -> SimpleNamespace:
        calls.append(args)
        return SimpleNamespace(returncode=0, stdout="pytest-token\n")

    monkeypatch.setattr(sync.subprocess, "run", fake_run)

    assert sync._github_token_default(remote_needed=False) is None
    assert sync._github_token_default(remote_needed=True) == "pytest-token"
    assert calls == [["gh", "auth", "token"]]


# ---------------------------------------------------------------------------
# BUG-102 regression: load_bug_files must skip individual corrupted files
# instead of aborting the entire scan.
# ---------------------------------------------------------------------------


def test_load_bug_files_skips_corrupted_json(tmp_path: Path) -> None:
    good = tmp_path / "20260523_BUG-501-good.json"
    good.write_text(json.dumps({
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": "BUG-501",
        "title": "good",
        "description": "ok",
        "module": "qe",
        "severity": "P1",
        "reproduce_command": "pytest",
        "fingerprint": "abc",
        "events": [],
    }), encoding="utf-8")

    bad = tmp_path / "20260523_BUG-CORRUPT.json"
    bad.write_bytes(b"\xef\xbb\xbf{not valid json")

    bugs = sync.load_bug_files(bugs_dir=tmp_path)
    bug_ids = {b["bug_id"] for b in bugs}
    assert "BUG-501" in bug_ids
    assert len(bugs) == 1


def test_load_bug_files_skips_non_object_json(tmp_path: Path) -> None:
    (tmp_path / "20260523_BUG-502.json").write_text("[]", encoding="utf-8")
    (tmp_path / "20260523_BUG-503.json").write_text(json.dumps({
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": "BUG-503",
        "title": "ok",
        "description": "ok",
        "module": "qe",
        "severity": "P1",
        "fingerprint": "abc",
        "events": [],
    }), encoding="utf-8")

    bugs = sync.load_bug_files(bugs_dir=tmp_path)
    bug_ids = {b["bug_id"] for b in bugs}
    assert bug_ids == {"BUG-503"}
