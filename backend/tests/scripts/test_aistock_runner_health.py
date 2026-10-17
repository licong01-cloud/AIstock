from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts import aistock_runner_health as health


def _online_runner(identifier, name, *roles):
    return {"id": identifier, "name": name, "os": "Windows", "status": "online", "busy": False,
            "labels": [{"name": label} for label in ("self-hosted", "Windows", *roles)]}


def test_nightly_preflight_requires_distinct_general_and_security_roles() -> None:
    root = Path(__file__).resolve().parents[3]
    workflow = (root / ".github" / "workflows" / "nightly.yml").read_text(encoding="utf-8")

    assert "--required-role general=self-hosted,windows,aistock-ci" in workflow
    assert "--required-role security=self-hosted,windows,aistock-ci-security" in workflow
    assert "--required-label aistock-ci" not in workflow


def test_runner_health_blocks_when_no_matching_runner() -> None:
    payload = health.build_runner_health_report(
        repo="licong01-cloud/AIstock",
        required_labels=["self-hosted", "windows"],
        runners_payload={"total_count": 0, "runners": []},
        runs_payload={"workflow_runs": []},
    )

    assert payload["schema_version"] == "aistock_runner_health_v1"
    assert payload["workflow_gate"] == "blocked"
    assert payload["online_matching_runners"] == []
    assert "no online GitHub Actions runner" in payload["blocking"][0]
    assert payload["production_gates"]["production_ddl_gate"] == "noop"


def test_runner_health_ready_with_online_self_hosted_windows_runner() -> None:
    payload = health.build_runner_health_report(
        required_labels=["self-hosted", "windows"],
        runners_payload={"total_count": 1, "runners": [_online_runner(10, "aistock-win-runner")]},
        runs_payload={"workflow_runs": []},
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["online_matching_runners"][0]["name"] == "aistock-win-runner"
    assert payload["blocking"] == []


def test_runner_health_requires_distinct_fast_and_security_roles() -> None:
    payload = health.build_runner_health_report(
        required_roles={
            "fast": ["self-hosted", "windows", "aistock-ci"],
            "security": ["self-hosted", "windows", "aistock-ci-security"],
        },
        runners_payload={"total_count": 2, "runners": [
            _online_runner(10, "aistock-fast", "aistock-ci"),
            _online_runner(11, "aistock-security", "aistock-ci-security")]},
        runs_payload={"workflow_runs": []},
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["runner_roles"]["fast"][0]["name"] == "aistock-fast"
    assert payload["runner_roles"]["security"][0]["name"] == "aistock-security"


def test_runner_health_rejects_one_runner_covering_both_parallel_roles() -> None:
    payload = health.build_runner_health_report(
        required_roles={
            "fast": ["self-hosted", "windows", "aistock-ci"],
            "security": ["self-hosted", "windows", "aistock-ci-security"],
        },
        runners_payload={"total_count": 1, "runners": [
            _online_runner(10, "aistock-combined", "aistock-ci", "aistock-ci-security")]},
        runs_payload={"workflow_runs": []},
    )

    assert payload["workflow_gate"] == "blocked"
    assert any("distinct online capacity" in item for item in payload["blocking"])


def test_runner_health_reports_stale_queued_runs() -> None:
    payload = health.build_runner_health_report(
        required_labels=["self-hosted", "windows"],
        stale_queued_minutes=30,
        now=datetime(2026, 5, 27, 15, 0, tzinfo=timezone.utc),
        runners_payload={"total_count": 0, "runners": []},
        runs_payload={
            "workflow_runs": [
                {
                    "id": 123,
                    "status": "queued",
                    "created_at": "2026-05-27T14:00:00Z",
                    "html_url": "https://github.com/licong01-cloud/AIstock/actions/runs/123",
                    "head_branch": "main",
                    "head_sha": "abc123",
                }
            ]
        },
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["stale_queued_runs"][0]["run_id"] == 123
    assert payload["stale_queued_runs"][0]["age_minutes"] == 60.0
    assert "queued nightly.yml run" in payload["warnings"][0]


def test_cancel_stale_scheduled_runs_revalidates_identity_and_active_jobs(monkeypatch) -> None:
    posts: list[str] = []

    def fake_get(path: str, *, token: str | None, timeout_seconds: int = 30):
        assert token == "token"
        if path.endswith("/runs/123"):
            return {"id": 123, "status": "queued", "event": "schedule", "head_branch": "main"}
        if path.endswith("/runs/123/jobs?per_page=100"):
            return {"jobs": [{"id": 10, "status": "queued"}]}
        raise AssertionError(path)

    monkeypatch.setattr(health, "_github_get", fake_get)
    monkeypatch.setattr(
        health,
        "_github_post",
        lambda path, *, token, timeout_seconds=30: posts.append(path),
    )
    receipt = health.cancel_stale_scheduled_runs(
        repo="licong01-cloud/AIstock",
        workflow="nightly.yml",
        stale_queued_minutes=30,
        current_run_id=999,
        current_event="schedule",
        expected_head_branch="main",
        token="token",
        now=datetime(2026, 9, 13, 3, 0, tzinfo=timezone.utc),
        runs_payload={
            "workflow_runs": [
                {
                    "id": 123,
                    "status": "queued",
                    "event": "schedule",
                    "head_branch": "main",
                    "created_at": "2026-09-13T01:00:00Z",
                },
                {
                    "id": 124,
                    "status": "queued",
                    "event": "workflow_dispatch",
                    "head_branch": "main",
                    "created_at": "2026-09-13T01:00:00Z",
                },
            ]
        },
    )

    assert receipt["workflow_gate"] == "ready"
    assert receipt["cancelled_run_ids"] == [123]
    assert receipt["skipped"] == [{"run_id": 124, "reason": "identity_mismatch"}]
    assert posts == ["/repos/licong01-cloud/AIstock/actions/runs/123/cancel"]


def test_cancel_stale_scheduled_runs_never_cancels_active_job(monkeypatch) -> None:
    def fake_get(path: str, *, token: str | None, timeout_seconds: int = 30):
        if path.endswith("/runs/123"):
            return {"id": 123, "status": "queued", "event": "schedule", "head_branch": "main"}
        if path.endswith("/runs/123/jobs?per_page=100"):
            return {"jobs": [{"id": 10, "status": "in_progress"}]}
        raise AssertionError(path)

    monkeypatch.setattr(health, "_github_get", fake_get)
    monkeypatch.setattr(health, "_github_post", lambda *args, **kwargs: pytest.fail("must not cancel"))
    receipt = health.cancel_stale_scheduled_runs(
        repo="licong01-cloud/AIstock",
        workflow="nightly.yml",
        stale_queued_minutes=30,
        current_run_id=999,
        current_event="schedule",
        expected_head_branch="main",
        token="token",
        now=datetime(2026, 9, 13, 3, 0, tzinfo=timezone.utc),
        runs_payload={
            "workflow_runs": [
                {
                    "id": 123,
                    "status": "queued",
                    "event": "schedule",
                    "head_branch": "main",
                    "created_at": "2026-09-13T01:00:00Z",
                }
            ]
        },
    )

    assert receipt["cancelled_run_ids"] == []
    assert receipt["skipped"] == [{"run_id": 123, "reason": "active_job"}]


def test_cancel_stale_scheduled_runs_returns_structured_failure_when_query_fails(monkeypatch) -> None:
    def fail_get(*args, **kwargs):
        raise RuntimeError("EOF")

    monkeypatch.setattr(health, "_github_get", fail_get)
    receipt = health.cancel_stale_scheduled_runs(
        repo="licong01-cloud/AIstock",
        workflow="nightly.yml",
        stale_queued_minutes=30,
        current_run_id=999,
        current_event="schedule",
        expected_head_branch="main",
        token="token",
        now=datetime(2026, 9, 13, 3, 0, tzinfo=timezone.utc),
    )

    assert receipt["workflow_gate"] == "blocked"
    assert receipt["action"] == "query_failed"
    assert receipt["errors"] == [{"run_id": None, "error": "EOF"}]


def test_runner_health_blocks_api_online_idle_runner_that_does_not_accept_work() -> None:
    payload = health.build_runner_health_report(
        workflow="codeql.yml",
        required_labels=["self-hosted", "windows", "aistock-ci-security"],
        stale_queued_minutes=10,
        now=datetime(2026, 9, 3, 4, 20, tzinfo=timezone.utc),
        runners_payload={
            "total_count": 1,
            "runners": [
                {
                    "id": 27,
                    "name": "aistock-security",
                    "os": "Windows",
                    "status": "online",
                    "busy": False,
                    "labels": [
                        {"name": "self-hosted"},
                        {"name": "Windows"},
                        {"name": "aistock-ci-security"},
                    ],
                }
            ],
        },
        runs_payload={
            "workflow_runs": [
                {
                    "id": 33713291574,
                    "status": "queued",
                    "created_at": "2026-09-03T04:07:00Z",
                }
            ]
        },
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["online_but_not_accepting_work"] is True
    assert any("stuck self-update or listener" in item for item in payload["blocking"])


def test_runner_health_does_not_attribute_other_role_queue_to_idle_runner() -> None:
    payload = health.build_runner_health_report(
        workflow="nightly.yml",
        required_labels=["self-hosted", "windows", "aistock-ci"],
        stale_queued_minutes=30,
        now=datetime(2026, 9, 12, 5, 0, tzinfo=timezone.utc),
        runners_payload={
            "total_count": 2,
            "runners": [
                {
                    "id": 26,
                    "name": "aistock-general",
                    "os": "Windows",
                    "status": "online",
                    "busy": False,
                    "labels": [
                        {"name": "self-hosted"},
                        {"name": "Windows"},
                        {"name": "aistock-ci"},
                    ],
                },
                {
                    "id": 27,
                    "name": "aistock-security",
                    "os": "Windows",
                    "status": "offline",
                    "busy": False,
                    "labels": [
                        {"name": "self-hosted"},
                        {"name": "Windows"},
                        {"name": "aistock-ci-security"},
                    ],
                },
            ],
        },
        runs_payload={
            "workflow_runs": [
                {
                    "id": 34532760217,
                    "status": "queued",
                    "created_at": "2026-09-10T21:32:39Z",
                }
            ]
        },
        jobs_payloads={
            "34532760217": {
                "jobs": [
                    {
                        "id": 103057261263,
                        "name": "Code intelligence daily graph refresh and summary",
                        "status": "queued",
                        "labels": ["self-hosted", "Windows", "aistock-ci-security"],
                    }
                ]
            }
        },
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["online_but_not_accepting_work"] is False
    assert payload["matching_stale_queued_runs"] == []
    assert payload["other_role_stale_queued_runs"][0]["run_id"] == 34532760217
    assert payload["stale_queued_runs"][0]["queued_jobs"][0]["labels"] == [
        "aistock-ci-security",
        "self-hosted",
        "windows",
    ]
    assert any("other runner roles" in item for item in payload["warnings"])


def test_runner_health_does_not_call_busy_runner_false_online() -> None:
    payload = health.build_runner_health_report(
        required_labels=["self-hosted", "windows", "aistock-ci"],
        stale_queued_minutes=10,
        now=datetime(2026, 9, 3, 4, 20, tzinfo=timezone.utc),
        runners_payload={
            "total_count": 1,
            "runners": [
                {
                    "id": 26,
                    "name": "aistock-ci",
                    "os": "Windows",
                    "status": "online",
                    "busy": True,
                    "labels": [
                        {"name": "self-hosted"},
                        {"name": "Windows"},
                        {"name": "aistock-ci"},
                    ],
                }
            ],
        },
        runs_payload={
            "workflow_runs": [
                {"id": 1, "status": "queued", "created_at": "2026-09-03T04:00:00Z"}
            ]
        },
    )

    assert payload["workflow_gate"] == "ready"
    assert payload["online_but_not_accepting_work"] is False


def test_render_markdown_includes_actionable_sections() -> None:
    payload = health.build_runner_health_report(
        runners_payload={"total_count": 0, "runners": []},
        runs_payload={"workflow_runs": []},
    )

    markdown = health.render_markdown(payload)

    assert "# AIstock Runner Health" in markdown
    assert "## Blocking" in markdown
    assert "## Next Actions" in markdown
    assert "production_ddl_gate" in markdown


def test_cli_writes_outputs_and_returns_blocked(tmp_path: Path, capsys, monkeypatch) -> None:
    runners = tmp_path / "runners.json"
    runs = tmp_path / "runs.json"
    output_json = tmp_path / "runner-health.json"
    output_md = tmp_path / "runner-health.md"
    runners.write_text(json.dumps({"total_count": 0, "runners": []}), encoding="utf-8")
    runs.write_text(json.dumps({"workflow_runs": []}), encoding="utf-8")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")

    rc = health.main(
        [
            "doctor",
            "--runners-json",
            str(runners),
            "--runs-json",
            str(runs),
            "--output-json",
            str(output_json),
            "--output-md",
            str(output_md),
        ]
    )

    assert rc == 2
    assert json.loads(output_json.read_text(encoding="utf-8"))["workflow_gate"] == "blocked"
    assert "AIstock Runner Health" in output_md.read_text(encoding="utf-8")
    captured = capsys.readouterr()
    assert json.loads(captured.out)["schema_version"] == "aistock_runner_health_v1"
    assert "::error::no online GitHub Actions runner" in captured.err


def test_resolve_github_token_prefers_env_over_gh(monkeypatch) -> None:
    monkeypatch.setenv("AISTOCK_RUNNER_HEALTH_TOKEN", "runner-token")
    monkeypatch.setenv("GH_TOKEN", "gh-token")

    token, source = health.resolve_github_token()

    assert token == "runner-token"
    assert source == "AISTOCK_RUNNER_HEALTH_TOKEN"


def test_resolve_github_token_uses_gh_auth_fallback(monkeypatch) -> None:
    for name in ("AISTOCK_RUNNER_HEALTH_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"):
        monkeypatch.delenv(name, raising=False)

    class Result:
        returncode = 0
        stdout = "fallback-token\n"

    monkeypatch.setattr(health.subprocess, "run", lambda *args, **kwargs: Result())

    token, source = health.resolve_github_token()

    assert token == "fallback-token"
    assert source == "gh_auth_token"


def test_resolve_github_token_reports_gh_subprocess_failure(monkeypatch) -> None:
    for name in ("AISTOCK_RUNNER_HEALTH_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"):
        monkeypatch.delenv(name, raising=False)

    def raise_timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("gh", 10)

    monkeypatch.setattr(health.subprocess, "run", raise_timeout)

    token, source = health.resolve_github_token()

    assert token is None
    assert source == "gh_auth_error:TimeoutExpired"


@pytest.mark.parametrize("utc,budget,event,admitted", [
    ("2026-10-12T19:07:00+00:00", 180, "schedule", True),
    ("2026-10-12T21:00:00+00:00", 120, "schedule", True),
    ("2026-10-12T21:00:01+00:00", 120, "schedule", False),
    ("2026-10-13T01:13:43+00:00", 180, "schedule", False),
    ("2026-10-12T15:59:59+00:00", 15, "schedule", False),
    ("2026-10-13T01:13:43+00:00", 180, "workflow_dispatch", True),
])
def test_background_admission_boundaries(utc, budget, event, admitted):
    receipt = health.scheduled_admission(
        event=event, budget_minutes=budget, source_head="a" * 40,
        run_id="123", now=datetime.fromisoformat(utc),
    )
    assert receipt["admitted"] is admitted
    assert receipt["validation_status"] == "not_executed"
    assert receipt["advances_validation_watermark"] is False
    assert receipt["source_head"] == "a" * 40


@pytest.mark.parametrize("field,value", [
    ("event", "pull_request"), ("budget_minutes", 0),
    ("budget_minutes", 421), ("source_head", "main"),
    ("run_id", ""), ("now", datetime(2026, 10, 13)),
])
def test_background_admission_rejects_invalid_context(field, value):
    kwargs = dict(event="schedule", budget_minutes=60, source_head="a" * 40,
                  run_id="123", now=datetime(2026, 10, 13, tzinfo=timezone.utc))
    kwargs[field] = value
    with pytest.raises(ValueError):
        health.scheduled_admission(**kwargs)


def test_background_admission_cli_is_local_and_records_deferral(monkeypatch, tmp_path):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 13, 1, tzinfo=timezone.utc)

    monkeypatch.setattr(health, "datetime", Clock)
    monkeypatch.setattr(health, "resolve_github_token", lambda: pytest.fail("network/auth used"))
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "outputs"))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary"))
    output = tmp_path / "receipt.json"
    assert health.main(["scheduled-admission", "--event", "schedule",
                        "--budget-minutes", "420", "--output-json", str(output)]) == 0
    receipt = json.loads(output.read_text(encoding="utf-8"))
    assert receipt["admitted"] is False
    assert "background_admitted=false" in (tmp_path / "outputs").read_text()
    assert "not_executed" in (tmp_path / "summary").read_text()


def test_background_workflows_recheck_and_skip_every_heavy_step():
    import yaml

    root = Path(__file__).resolve().parents[3]
    for filename in ("nightly.yml", "codeql.yml", "code-intelligence-refresh.yml"):
        jobs = yaml.safe_load((root / ".github/workflows" / filename).read_text(encoding="utf-8"))["jobs"]
        preflight = jobs.get("runner-preflight", jobs.get("security-runner-preflight"))
        assert "background_admitted" in preflight["outputs"]
        for job in jobs.values():
            if not isinstance(job["runs-on"], list):
                continue
            steps = job["steps"]
            assert steps[0]["id"] == "background_window"
            assert f'--budget-minutes {job["timeout-minutes"]}' in steps[0]["run"] or filename == "nightly.yml"
            assert "background_admitted == 'true'" in job["if"]
            assert all("steps.background_window.outputs.background_admitted == 'true'" in step["if"]
                       for step in steps[1:])
    nightly = (root / ".github/workflows/nightly.yml").read_text(encoding="utf-8")
    assert "needs.nightly-l3.outputs.background_admitted != 'false'" in nightly
    assert "&& 'deferred' || needs.nightly-l3.result" in nightly


@pytest.mark.parametrize("utc,cycle,expected", [
    ("2026-10-14T19:07:00+00:00", "2026-10-15", True),
    ("2026-10-14T23:14:00+00:00", "2026-10-15", False),
    ("2026-10-14T19:07:00+00:00", "2026-10-14", None),
    ("2026-10-14T19:07:00+00:00", "2026-10-16", None),
    ("2026-10-14T19:07:00+00:00", None, None),
])
def test_local_automatic_dispatch_never_gets_manual_window_exemption(utc, cycle, expected):
    kwargs = dict(event="repository_dispatch", budget_minutes=180, source_head="a" * 40,
                  run_id="123", now=datetime.fromisoformat(utc), cycle_date=cycle)
    if expected is None:
        with pytest.raises(ValueError):
            health.scheduled_admission(**kwargs)
    else:
        assert health.scheduled_admission(**kwargs)["admitted"] is expected


@pytest.mark.parametrize("clock,posts", [
    ("2026-10-14T18:59:00+00:00", 0), ("2026-10-14T19:07:00+00:00", 1),
    ("2026-10-14T20:00:00+00:00", 1), ("2026-10-14T20:00:01+00:00", 0),
    ("2026-10-15T01:00:00+00:00", 0),
])
def test_wall_clock_tick_is_bounded_and_idempotent(monkeypatch, tmp_path, clock, posts):
    monkeypatch.setattr(health, "SCHEDULED_WORKFLOWS", {"nightly.yml": ("aistock_nightly", 3, 7, 180)})
    calls = []
    def request(path, payload=None):
        calls.append((path, payload))
        return {"workflow_runs": []} if payload is None else {}
    kwargs = dict(state_root=tmp_path, now=datetime.fromisoformat(clock), request=request)
    health.dispatch_scheduled_cycle(**kwargs)  # Dry-run does not query auth/network or write.
    assert not calls and not list(tmp_path.iterdir())
    for _ in range(2):
        result = health.dispatch_scheduled_cycle(**kwargs, apply=True)
        assert result["validation_status"] == "not_executed"
    assert sum(payload is not None for _, payload in calls) == posts


@pytest.mark.parametrize("failure", [TimeoutError, RuntimeError])
def test_uncertain_dispatch_is_not_replayed_and_run_reconciliation_is_exact(monkeypatch, tmp_path, failure):
    monkeypatch.setattr(health, "SCHEDULED_WORKFLOWS", {"nightly.yml": ("aistock_nightly", 3, 7, 180)})
    posts, runs = [], []
    def request(path, payload=None):
        if payload is not None:
            posts.append(payload)
            raise failure("transport ambiguity")
        return {"workflow_runs": runs}
    kwargs = dict(state_root=tmp_path, apply=True, now=datetime.fromisoformat("2026-10-14T19:07:00+00:00"),
                  request=request)
    assert health.dispatch_scheduled_cycle(**kwargs)["blocking"]
    assert health.dispatch_scheduled_cycle(**kwargs)["blocking"]
    assert len(posts) == 1
    runs.append(dict(display_title="AIstock schedule nightly.yml 2026-10-15", id=42,
                     event="repository_dispatch", head_branch="main", head_sha="bad", status="queued"))
    assert health.dispatch_scheduled_cycle(**kwargs)["blocking"]
    runs.clear()
    runs.append(dict(display_title="AIstock schedule nightly.yml 2026-10-15", id=42,
                     event="repository_dispatch", head_branch="main", head_sha="a" * 40, status="queued"))
    report = health.dispatch_scheduled_cycle(**kwargs)
    assert not report["blocking"] and report["workflows"][0]["run_id"] == 42
    assert report["workflows"][0]["validation_status"] == "not_executed"
    assert len(posts) == 1


def test_wall_clock_workflows_have_single_trigger_authority_and_receipt_reuse():
    import yaml
    root = Path(__file__).resolve().parents[3]
    for filename, (event, *_rest) in health.SCHEDULED_WORKFLOWS.items():
        text = (root / ".github/workflows" / filename).read_text(encoding="utf-8")
        workflow = yaml.safe_load(text)
        triggers = workflow.get("on", workflow.get(True))
        assert "schedule" not in triggers
        assert triggers["repository_dispatch"]["types"] == [event]
        assert "workflow_dispatch" in triggers and f"AIstock schedule {filename}" in workflow["run-name"]
    nightly = (root / ".github/workflows/nightly.yml").read_text(encoding="utf-8")
    assert "--event schedule" not in nightly
    installer = (root / "scripts/configure_aistock_github_runner.ps1").read_text(encoding="utf-8")
    assert "-LogonType Interactive -RunLevel Limited" in installer
    assert "pythonw.exe" in installer and "-MultipleInstances IgnoreNew" in installer


@pytest.mark.parametrize("event,title,expected", [
    ("schedule", "", True),
    ("repository_dispatch", "AIstock schedule nightly.yml 2026-10-15", True),
    ("repository_dispatch", "AIstock schedule nightly.yml 2026-02-31", False),
    ("repository_dispatch", "AIstock schedule codeql.yml 2026-10-15", False),
    ("workflow_dispatch", "AIstock schedule nightly.yml 2026-10-15", False),
])
def test_watchdog_only_recognizes_exact_automatic_cycle(event, title, expected):
    assert health._automatic_run({"event": event, "display_title": title}, "nightly.yml") is expected


def test_dispatch_lock_has_single_owner_and_releases_on_exception(tmp_path):
    with pytest.raises(RuntimeError), health._dispatch_lock(tmp_path):
        with pytest.raises(OSError), health._dispatch_lock(tmp_path):
            pytest.fail("concurrent owner admitted")
        raise RuntimeError("simulated crash")
    with health._dispatch_lock(tmp_path):
        pass


def test_dispatch_rest_route_preserves_payload_and_existing_proxy_exclusions(monkeypatch):
    calls = []
    monkeypatch.setenv("NO_PROXY", "localhost")
    monkeypatch.setattr(health.subprocess, "run", lambda args, **kwargs:
                        (calls.append((args, kwargs)) or subprocess.CompletedProcess(args, 0, "")))
    health._dispatch_api("repos/example/repo/dispatches", {"event_type": "scheduled"})
    args, kwargs = calls[0]
    assert args[-3:] == ["POST", "--input", "-"]
    assert json.loads(kwargs["input"]) == {"event_type": "scheduled"}
    assert "localhost" in kwargs["env"]["NO_PROXY"] and "api.github.com" in kwargs["env"]["NO_PROXY"]
    assert kwargs["timeout"] == 30 and kwargs["capture_output"]


@pytest.mark.parametrize("action", ["aistock_nightly", "unknown"])
def test_repository_event_cli_requires_registered_type(monkeypatch, tmp_path, action):
    event = tmp_path / "event.json"
    event.write_text(json.dumps({"action": action, "client_payload": {
        "cycle_date": datetime.now(timezone.utc).astimezone(health.timezone(health.timedelta(hours=8))).date().isoformat()}}))
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))
    assert health.main(["scheduled-admission", "--event", "repository_dispatch", "--source-head", "a" * 40,
                        "--run-id", "123", "--budget-minutes", "180"]) == (0 if action == "aistock_nightly" else 2)


@pytest.mark.parametrize("mode", ["ready", "inactive", "apply"])
def test_scheduler_access_check_cannot_dispatch(monkeypatch, tmp_path, mode):
    calls = []
    def request(path, payload=None):
        assert payload is None
        calls.append(path)
        return {"state": "disabled" if mode == "inactive" else "active",
                "path": ".github/workflows/" + path.rsplit("/", 1)[-1]}
    monkeypatch.setattr(health, "_dispatch_api", request)
    args = ["dispatch-scheduled", "--state-root", str(tmp_path), "--check-access"]
    result = health.main(args + (["--apply"] if mode == "apply" else []))
    assert result == (0 if mode == "ready" else 2)
    assert len(calls) == (3 if mode == "ready" else 1 if mode == "inactive" else 0)
    assert not list(tmp_path.iterdir())
