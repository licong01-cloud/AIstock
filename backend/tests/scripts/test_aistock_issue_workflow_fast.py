from __future__ import annotations

import argparse
import hashlib
import io
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import scripts.aistock_issue_workflow as workflow
from scripts.aistock_bug_id_allocator import compact_terminal_reservation


@pytest.mark.parametrize("selection,plans", [("l0", ["l0"]), ("l0 validation_catalog_integrity", ["l0", "validation_catalog_integrity"]), ("l0 l0 -- tests", ["l0"]), ("l0,validation_catalog_integrity", []), ("l0; unknown", []), ("l0 -s validation_catalog_integrity", []), ("l0 -- -k smoke", ["l0"])])
def test_validation_receipt_explicit_nox_sessions(monkeypatch, tmp_path, selection, plans):
    monkeypatch.setattr(workflow, "_assert_task_git_identity", lambda _: None)
    monkeypatch.setattr(workflow, "_git", lambda *args, **kwargs: "a" * 40)
    command = "python -m nox -s " + selection
    receipts, errors = workflow._build_validation_receipts([command + " -> passed"], root=tmp_path)
    assert [receipt["plan"] for receipt in receipts] == plans
    assert bool(errors) == (not plans)
    assert len({receipt["receipt_id"] for receipt in receipts}) == len(plans)
    assert all(receipt["command"] == command and receipt["commit"] == "a" * 40 for receipt in receipts)
    coverage = workflow._validation_receipt_plan_coverage(validation={"required_plans": plans}, receipts=receipts)
    assert coverage["missing_required_plans"] == []
    failed, errors = workflow._build_validation_receipts([command + " -> FAILED, 1 passed"], root=tmp_path)
    assert failed == [] and errors


@pytest.mark.parametrize("attempts,delay", [(16, 30), (6, 10), (1, 30), (4, 0), (0, -1)])
def test_required_check_schedule_preserves_budget(attempts, delay):
    fixed = workflow._required_check_poll_schedule(attempts, delay)
    adaptive = workflow._required_check_poll_schedule(attempts, delay, adaptive=True)
    assert fixed == (max(0, delay),) * (max(1, attempts) - 1)
    assert sum(adaptive) == sum(fixed)
    assert all(0 <= value <= max(0, delay) for value in adaptive)
    assert len(adaptive) <= len(fixed) + 3
    if attempts > 1 and delay > 0:
        assert adaptive[0] == min(5, delay)


@pytest.mark.parametrize("outcome,expected_calls", [("passed", 2), ("failed", 1), ("pending", 18)])
def test_adaptive_required_checks_stop_early_or_fail_closed(monkeypatch, outcome, expected_calls):
    calls, waits = [], []
    payload = {"headRefOid": "pinned-task-head"}
    def read(url, **kwargs):
        assert kwargs["payload"] is payload and url == "https://example.invalid/pr/1"
        calls.append(url)
        state = "pending" if outcome == "pending" or (outcome == "passed" and len(calls) == 1) else outcome
        return {"state": state}, None
    monkeypatch.setattr(workflow, "_merge_required_check_result_with_transport_fallback", read)
    monkeypatch.setattr(workflow, "_required_pr_check_summary", lambda result: {key: (["CI verdict"] if key == result["state"] else []) for key in ("failed", "pending", "passed", "non_blocking")})
    monkeypatch.setattr(workflow.time, "sleep", waits.append)
    _, fallback, summary, history = workflow._await_required_pr_checks("https://example.invalid/pr/1", payload=payload, attempts=16, delay_seconds=30, adaptive=True)
    assert fallback is None and len(calls) == len(history) == expected_calls
    assert summary[outcome] == ["CI verdict"]
    assert waits == ([] if outcome == "failed" else [5] if outcome == "passed" else list(workflow._required_check_poll_schedule(16, 30, adaptive=True)))


@pytest.fixture
def cli_task_worktree(tmp_path, monkeypatch):
    base, task = tmp_path / "authority", tmp_path / "task"
    base.mkdir()

    def git(root, *args):
        return subprocess.check_output(["git", "-C", str(root), *args], text=True, stderr=subprocess.STDOUT, timeout=15).strip()

    git(base, "init", "-q")
    # Git identity is supplied per command; the shared baseline can be empty.
    git(base, "-c", "user.email=workflow-test@example.invalid", "-c", "user.name=workflow-test", "commit", "--allow-empty", "-qm", "baseline")
    git(base, "update-ref", "refs/remotes/origin/main", "HEAD")
    git(base, "worktree", "add", "-qb", "bug/task-context", str(task))
    (task / "task.txt").write_text("task", encoding="utf-8")
    git(task, "add", ".")
    git(task, "-c", "user.email=workflow-test@example.invalid", "-c", "user.name=workflow-test", "commit", "-qm", "task change")
    monkeypatch.setattr(workflow, "REPO_ROOT", base)
    monkeypatch.setattr(workflow, "BUGS_ROOT", base / "tests/aistock_validation/bugs")
    monkeypatch.setattr(workflow, "SCRIPT_ROOT", base, raising=False)
    monkeypatch.chdir(task)
    return base, task, git


@pytest.mark.parametrize("create", [False, True])
def test_canonical_cli_uses_task_for_diff_receipt_branch_and_pr(cli_task_worktree, monkeypatch, create):
    base, task, git = cli_task_worktree
    original_root = workflow.REPO_ROOT
    original_flow_root = workflow.flow.REPO_ROOT
    original_exists = Path.exists
    monkeypatch.setattr(Path, "exists", lambda path: False if path == Path("F:/Dev/AIstock") else original_exists(path))
    monkeypatch.delenv("AISTOCK_CANONICAL_ROOT", raising=False)
    monkeypatch.delenv("AISTOCK_ROOT", raising=False)
    (task / "nested").mkdir()
    monkeypatch.chdir(task / "nested")
    state = task / "tmp/issue_workflow/BUG-999/state.json"
    state.parent.mkdir(parents=True)
    state.write_text('{"worktree":"real-task"}', encoding="utf-8")
    bug = task / "tests/aistock_validation/bugs/20261003_BUG-999.json"
    bug.parent.mkdir(parents=True)
    bug.write_text('{"bug_id":"BUG-999","status":"open"}', encoding="utf-8")
    calls = []

    def handler(_args):
        assert workflow.REPO_ROOT == workflow.flow.REPO_ROOT == task
        assert workflow._canonical_root() == base
        assert workflow.flow.BUGS_ROOT == task / "tests/aistock_validation/bugs"
        assert workflow.flow.TEST_PLANS == task / "tests/aistock_validation/catalog/test_plans.yaml"
        assert workflow.BUGS_ROOT == workflow.flow.BUGS_ROOT
        assert workflow.flow.FILE_OWNERSHIP == task / "tests/aistock_validation/catalog/file_ownership.yaml"
        assert workflow._load_state("BUG-999")["worktree"] == "real-task"
        assert workflow.find_bug_record("BUG-999")[1] == bug
        assert workflow._finish_changed_files("origin/main", "HEAD") == ["task.txt", "tests/", "tmp/"]
        receipts, errors = workflow._build_validation_receipts(["python -m nox -s l0 -> passed"], root=workflow.REPO_ROOT)
        assert not errors and receipts[0]["commit"] == git(task, "rev-parse", "HEAD")
        assert receipts[0]["commit"] != git(base, "rev-parse", "HEAD")
        finish = {"validation_evidence": ["passed"], "validation_receipts": receipts, "pr_body_path": "tmp/body.md"}
        monkeypatch.setattr(workflow, "_pr_worktree_guard", lambda: {"blocking": []})
        monkeypatch.setattr(workflow, "_pre_pr_gate", lambda **kwargs: {"workflow_gate": "passed", "blocking": []})
        monkeypatch.setattr(workflow, "_create_pr_with_transport_fallback", lambda **kwargs: calls.append(kwargs) or {"ok": True, "stdout": "https://github.com/test/repo/pull/1"})
        monkeypatch.setattr(workflow, "_append_event", lambda *args, **kwargs: None)
        monkeypatch.setattr(workflow, "_write_state", lambda *args, **kwargs: None)
        workflow._maybe_create_pr(bug_id="BUG-999", finish=finish, push=False, create_pr=True, watch_ci=False, pr_title=None)
        assert calls[0]["root"] == task and calls[0]["branch"] == "bug/task-context"
        assert calls[0]["expected_head"] == receipts[0]["commit"]
        state.write_text(json.dumps({"planned_worktree": str(task), "planned_branch": "bug/task-context"}), encoding="utf-8")
        plan = workflow._maybe_create_worktree(
            record={"bug_id": "BUG-999", "title": "task"}, bug_id="BUG-999",
            source_bug_json=bug, create=create, dry_run=False, task_slug=None,
        )
        assert plan["reused"] is True and not plan.get("created")
        assert plan["branch"] == "bug/task-context"
        assert workflow._actual_and_planned_worktree(plan) == (str(task), None)
        return 0

    args = SimpleNamespace(command="run", mode="pr", func=handler)
    monkeypatch.setattr(workflow, "build_parser", lambda: SimpleNamespace(parse_args=lambda _: args))
    assert workflow.main([]) == 0
    assert workflow.REPO_ROOT == original_root
    assert workflow.flow.REPO_ROOT == original_flow_root


@pytest.mark.parametrize("failure", ["unknown", "foreign", "git_override", "head_drift", "branch_drift", "receipt_mismatch", "digest_mismatch", "head_argument", "registered_branch_drift", "workflow_error"])
def test_canonical_cli_task_context_fails_closed(cli_task_worktree, monkeypatch, capsys, failure):
    base, task, git = cli_task_worktree
    original_root = workflow.REPO_ROOT
    invoked = []

    def handler(_args):
        invoked.append(True)
        if failure == "workflow_error":
            raise workflow.WorkflowError("target validation failed")
        if failure == "registered_branch_drift":
            state = task / "tmp/issue_workflow/BUG-999/state.json"
            state.parent.mkdir(parents=True)
            state.write_text(json.dumps({"worktree": str(task), "branch": "wrong/branch"}), encoding="utf-8")
            workflow._maybe_create_worktree(
                record={"bug_id": "BUG-999"}, bug_id="BUG-999", source_bug_json=task / "BUG-999.json",
                create=False, dry_run=False, task_slug=None,
            )
            pytest.fail("registered branch drift must fail closed")
        if failure == "head_drift":
            git(task, "-c", "user.email=workflow-test@example.invalid", "-c", "user.name=workflow-test", "commit", "--allow-empty", "-qm", "concurrent head change")
        if failure == "branch_drift":
            git(task, "branch", "-m", "renamed-task")
        root = workflow.REPO_ROOT
        if failure == "head_argument":
            workflow.build_finish_plan(bug_id="BUG-999", issue_json=None, changed_files=None,
                                       base="origin/main", head=git(base, "rev-parse", "HEAD"),
                                       validation_evidence=[], plan_only=True, allow_missing_evidence=False)
        if failure in {"receipt_mismatch", "digest_mismatch"}:
            finish = {"validation_evidence": ["passed"], "validation_receipts": [{"commit": git(base, "rev-parse", "HEAD")}]}
            if failure == "digest_mismatch":
                finish["validation_receipts"][0]["commit"] = git(task, "rev-parse", "HEAD")
            workflow._check_pr_receipt_identity(finish, root=root)
        else:
            workflow._build_validation_receipts(["python -m nox -s l0 -> passed"], root=root)
        return 0

    if failure in {"unknown", "foreign"}:
        other = task.parent / failure
        other.mkdir()
        if failure == "foreign":
            git(other, "init", "-q")
        monkeypatch.chdir(other)
    if failure == "git_override":
        monkeypatch.setenv("GIT_DIR", str(base / ".git"))
    args = SimpleNamespace(command="finish", func=handler)
    monkeypatch.setattr(workflow, "build_parser", lambda: SimpleNamespace(parse_args=lambda _: args))
    assert workflow.main([]) == 2
    assert workflow.REPO_ROOT == original_root
    assert capsys.readouterr().err
    assert bool(invoked) == (failure not in {"unknown", "foreign", "git_override"})


@pytest.mark.parametrize("executable", ["gh", "C:/tools/gh.exe"])
@pytest.mark.parametrize("existing", ["localhost", "api.github.com", "*"])
def test_gh_environment_bypasses_only_api_and_preserves_parent(monkeypatch, executable, existing):
    parent = {"NO_PROXY": existing, "no_proxy": "127.0.0.1", "HTTPS_PROXY": "http://127.0.0.1:7896"}
    monkeypatch.setattr(workflow.os, "environ", parent)
    monkeypatch.setattr(workflow.platform, "system", lambda: "Windows")
    env = workflow._subprocess_env([executable, "api", "graphql"])
    assert env is not None and env is not parent
    assert env["NO_PROXY"] == env["no_proxy"]
    assert set(env["NO_PROXY"].split(",")) == {existing, "127.0.0.1", "api.github.com"}
    assert env["HTTPS_PROXY"] == parent["HTTPS_PROXY"]
    assert parent == {"NO_PROXY": existing, "no_proxy": "127.0.0.1", "HTTPS_PROXY": "http://127.0.0.1:7896"}


@pytest.mark.parametrize(("system", "override"), [("Linux", "1"), ("Darwin", "1"), ("Windows", "0")])
def test_gh_route_keeps_non_windows_and_explicit_proxy_override(monkeypatch, system, override):
    parent = {"NO_PROXY": "localhost", "HTTPS_PROXY": "http://127.0.0.1:7896", "AISTOCK_GITHUB_API_DIRECT": override}
    monkeypatch.setattr(workflow.os, "environ", parent)
    monkeypatch.setattr(workflow.platform, "system", lambda: system)
    assert workflow._subprocess_env(["gh"]) == parent


@pytest.mark.parametrize("args", [[], ["python"], ["curl.exe"], ["not-gh"]])
def test_non_gh_commands_keep_inherited_network_environment(args):
    assert workflow._subprocess_env(args) is None


@pytest.mark.parametrize("command", ["promote-ci-issue", "ci-issue-janitor"])
def test_metadata_cli_starts_without_process_dependency(command):
    import sys

    code = (
        "import importlib.abc, runpy, sys\n"
        "class NoPsutil(importlib.abc.MetaPathFinder):\n"
        "    def find_spec(self, fullname, path=None, target=None):\n"
        "        if fullname == 'psutil':\n"
        "            raise ModuleNotFoundError('No psutil in metadata runner', name='psutil')\n"
        "sys.meta_path.insert(0, NoPsutil())\n"
        f"sys.argv = ['aistock_issue_workflow.py', {command!r}, '--help']\n"
        "runpy.run_path('scripts/aistock_issue_workflow.py', run_name='__main__')\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    assert command in result.stdout


def test_process_probe_fails_closed_without_prebuilt_psutil(monkeypatch):
    monkeypatch.setattr(workflow, "psutil", None)
    with pytest.raises(workflow.WorkflowError, match="requires the prebuilt psutil"):
        workflow._monthly_release_worker_process_snapshot({})


def _monthly_ready_payload() -> dict[str, Any]:
    from backend.services.dataset_release.monthly_unified import STAGES
    return {
        "schema_version": "aistock_monthly_release_status_v1",
        "data": {
            "schema_version": "aistock_monthly_release_state_v1",
            "operation_id": "dmr_" + "a" * 32,
            "status": "READY_TO_ACTIVATE", "attempt": 1,
            "plan_sha256": "b" * 64, "ready_receipt_sha256": "c" * 64,
            "cancel_requested": False, "last_error": None, "current_stage": None,
            "checkpoints": dict.fromkeys(STAGES, True),
        },
    }


def _monthly_verdict(payload):
    return _business_semantic(
        "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32,
        payload, "d" * 64,
    )


def test_monthly_ready_probe_requires_operation_bound_success() -> None:
    verdict = _monthly_verdict(_monthly_ready_payload())
    assert verdict["contract_id"] == "monthly_release_ready"
    assert verdict["verdict"] == "passed"


@pytest.mark.parametrize(("key", "value"), [
    ("operation_id", "dmr_" + "f" * 32), ("status", "FAILED"),
    ("status", "SOURCE_READY"), ("status", "ACTIVATED_VERIFY_FAILED"),
    ("status", []),
    ("cancel_requested", True), ("cancel_requested", 0), ("attempt", True),
    ("ready_receipt_sha256", ""), ("plan_sha256", "invalid"),
    ("current_stage", "SOURCE"), ("last_error", {"code": "SOURCE_INCOMPLETE"}),
])
def test_monthly_probe_rejects_cross_operation_or_unready_state(key, value) -> None:
    payload = _monthly_ready_payload()
    payload["data"][key] = value
    assert _monthly_verdict(payload)["verdict"] == "failed"


@pytest.mark.parametrize("case", ["missing", "non_bool", "extra", "failed", "outer_schema", "state_schema"])
def test_monthly_probe_checkpoint_and_schema_closure(case) -> None:
    payload = _monthly_ready_payload()
    points = payload["data"]["checkpoints"]
    if case == "missing":
        del points["SOURCE"]
    elif case == "non_bool":
        points["SOURCE"] = 1
    elif case == "extra":
        points["UNREVIEWED"] = True
    elif case == "failed":
        points["SOURCE"] = False
    elif case == "outer_schema":
        payload["schema_version"] = "other"
    else:
        payload["data"]["schema_version"] = "other"
    assert _monthly_verdict(payload)["verdict"] == "failed"


def test_monthly_read_only_probe_uses_scoped_operator_file(monkeypatch, tmp_path) -> None:
    token = hashlib.sha256(b"monthly-probe-credential-fixture").hexdigest()
    secret_file = tmp_path / "operator.token"
    secret_file.write_text(token, encoding="utf-8")
    monkeypatch.setenv("DATASET_RELEASE_OPERATOR_TOKEN_FILE", str(secret_file))
    captured = []

    def open_probe(request, **_kwargs):
        captured.append(request)
        return io.BytesIO(b'{"status":"FAILED"}')

    monkeypatch.setattr(workflow, "_open_read_only_url", open_probe)
    url = "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32
    receipt = workflow._read_only_http_probe("business_smoke_ref", url, allowed_origins=["http://127.0.0.1:8001"])
    assert captured[0].get_header("X-dataset-release-operator-token") == token
    assert captured[0].method == "GET"
    assert token not in json.dumps(receipt)
    assert receipt["_response_body"] == '{"status":"FAILED"}'  # HTTP success is not business success.


@pytest.mark.parametrize("url", [
    "http://node1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32,
    "http://127.0.0.1:8001/health",
    "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32 + "/activate",
    "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32 + "?redirect=evil",
])
def test_other_probes_never_read_or_forward_operator_secret(monkeypatch, url) -> None:
    from scripts import monthly_unified_dataset_release as release
    monkeypatch.setattr(release, "_token", lambda: pytest.fail("must not load secret"))
    captured = []
    monkeypatch.setattr(workflow, "_open_read_only_url", lambda request, **kw: (captured.append(request) or io.BytesIO(b"{}")))
    origin = workflow._normalized_http_origin(url)
    workflow._read_only_http_probe("business_smoke_ref", url, allowed_origins=[origin])
    assert all(request.get_header("X-dataset-release-operator-token") is None for request in captured)


def test_monthly_probe_missing_secret_fails_closed_before_network(monkeypatch) -> None:
    monkeypatch.delenv("DATASET_RELEASE_OPERATOR_TOKEN_FILE", raising=False)
    monkeypatch.setattr(workflow, "_open_read_only_url", lambda *a, **kw: pytest.fail("must not call API"))
    url = "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32
    receipt = workflow._read_only_http_probe("business_smoke_ref", url, allowed_origins=["http://127.0.0.1:8001"])
    assert receipt["status"] == "blocked"


@pytest.mark.parametrize("reflect_in_error", [False, True])
def test_authenticated_probe_never_records_reflected_secret(monkeypatch, reflect_in_error) -> None:
    from scripts import monthly_unified_dataset_release as release
    import urllib.error
    token = hashlib.sha256(b"monthly-probe-reflected-fixture").hexdigest()
    monkeypatch.setattr(release, "_token", lambda: token)

    def open_probe(*args, **kwargs):
        if reflect_in_error:
            raise urllib.error.URLError(token)
        return io.BytesIO(json.dumps({"echo": token}).encode())

    monkeypatch.setattr(workflow, "_open_read_only_url", open_probe)
    url = "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32 + "/receipts"
    receipt = workflow._read_only_http_probe("business_smoke_ref", url, allowed_origins=["http://127.0.0.1:8001"])
    assert receipt["status"] == "failed"
    assert token not in json.dumps(receipt)
    assert "_response_body" not in receipt


def test_pre_pr_gate_reuses_exact_ci_classifier_and_blocks_before_push(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(workflow, "_git_status_paths", lambda _root: [])
    monkeypatch.setattr(
        workflow,
        "_run_ci_changed_file_classifier",
        lambda _paths, root: {
            "workflow_gate": "blocked",
            "classification": "unexecuted_test_blocked",
            "blocking": ["changed test files are not executed by any selected CI plan: ['backend/tests/new_test.py']"],
        },
    )

    gate = workflow._pre_pr_gate(
        finish={
            "changed_files": ["backend/tests/new_test.py"],
            "scope_check": {"status": "passed"},
            "fast_path": {"ownership": {}},
            "closure_ready": True,
        },
        validation_evidence=["pytest backend/tests/new_test.py -> passed"],
        root=Path.cwd(),
        run_lint=False,
    )

    assert gate["workflow_gate"] == "blocked"
    assert gate["ci_classifier"]["classification"] == "unexecuted_test_blocked"
    assert any("local CI classifier" in item for item in gate["blocking"])


def _local_data_payload(overview: bool = False) -> dict[str, Any]:
    row = {'data_kind': 'adj_factor', 'stats_max_date': '2026-08-11',
           'audit_ready_date': '2026-09-29', 'ready_date': '2026-09-29',
           'audit_quality_status': 'ok', 'physical_max_date': None,
           'physical_max_date_source': 'not_probed', 'stats_date_source': 'data_stats_cache',
           'readiness_source': 'dataset_date_refresh_audit', 'cache_state': 'stale',
           'readiness_status': 'audit_success', 'operator_action_required': False}
    data: dict[str, Any] = {'items': [row]}
    if overview:
        data = {'datasets': [row], 'dataset_count': 1, 'stale_dataset_count': 1,
                'stale_stats_cache_count': 1, 'readiness_unknown_count': 0,
                'quality_blocked_dataset_count': 0, 'running_job_count': 0,
                'active_alert_count': 0, 'blocked_target_count': 0,
                'retry_target_count': 0, 'status': 'yellow'}
    return {'success': True, 'operation': 'local_data_health_overview' if overview else 'local_data_list_data_stats',
            'risk_level': 'read_only', 'data': data}


@pytest.mark.parametrize('overview', [False, True])
def test_local_data_freshness_separates_cache_and_readiness(overview: bool) -> None:
    endpoint = 'overview' if overview else 'data-stats'
    _, verdict = workflow._evaluate_business_smoke_semantics(
        f'http://127.0.0.1:8001/api/v1/local-data/{endpoint}',
        json.dumps(_local_data_payload(overview)), response_sha256='a'*64)
    assert verdict['verdict'] == 'passed'
    assert verdict['contract_id'] == 'local_data_freshness'
    assert verdict['facts']['stale'] == 1


@pytest.mark.parametrize(('key', 'value'), [
    ('physical_max_date', '2026-09-29'), ('physical_max_date_source', 'cache'),
    ('stats_date_source', 'live'), ('readiness_source', 'cache'),
    ('ready_date', '2026-08-11'), ('cache_state', 'fresh'),
    ('readiness_status', 'unknown'), ('operator_action_required', 0),
    ('audit_ready_date', '2026-09-31'), ('stats_max_date', '2026-8-11'),
    ('audit_quality_status', {}),
])
def test_local_data_freshness_rejects_conflated_evidence(key: str, value: Any) -> None:
    payload = _local_data_payload()
    payload['data']['items'][0][key] = value
    status, _, _ = workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/data-stats')
    assert status == 'failed'


@pytest.mark.parametrize('state', ['unknown', 'quality_blocked'])
def test_local_data_freshness_preserves_non_ready_states(state: str) -> None:
    payload = _local_data_payload()
    row = payload['data']['items'][0]
    row['readiness_status'] = state
    if state == 'unknown':
        row.update(audit_ready_date=None, ready_date=None, cache_state='audit_missing')
    else:
        row['audit_quality_status'] = 'low_coverage'
    status, _, facts = workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/data-stats')
    assert status == 'passed' and facts[state] == 1


@pytest.mark.parametrize(('key', 'value'), [('success', False), ('operation', 'wrong'), ('risk_level', 'write')])
def test_local_data_freshness_requires_read_only_envelope(key: str, value: Any) -> None:
    payload = _local_data_payload()
    payload[key] = value
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/data-stats')[0] == 'failed'


def test_local_data_overview_validates_counters_without_hiding_real_alerts() -> None:
    payload = _local_data_payload(True)
    data = payload['data']
    data.update(status='red', blocked_target_count=4, active_alert_count=4)
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/overview')[0] == 'passed'
    data['status'] = 'green'
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/overview')[0] == 'failed'
    data.update(status='red', dataset_count=2, readiness_unknown_count=1)
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/overview')[0] == 'passed'
    data['stale_stats_cache_count'] = 0
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/overview')[0] == 'failed'


def test_local_data_freshness_rejects_missing_and_duplicate_dataset_evidence() -> None:
    payload = _local_data_payload()
    payload['data']['items'].append(dict(payload['data']['items'][0]))
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/data-stats')[0] == 'failed'
    payload['data']['items'] = []
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/data-stats')[0] == 'failed'


@pytest.mark.parametrize(('key', 'value'), [('status', {}), ('dataset_count', True),
                                         ('readiness_unknown_count', -1)])
def test_local_data_overview_rejects_malformed_summary(key: str, value: Any) -> None:
    payload = _local_data_payload(True)
    payload['data'][key] = value
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/overview')[0] == 'failed'


def _result(*, ok: bool = True, stdout: str = "", stderr: str = "", returncode: int = 0) -> dict[str, Any]:
    return {"ok": ok, "stdout": stdout, "stderr": stderr, "returncode": returncode}


_METRICS_PROBE = (
    "http://127.0.0.1:8001/api/v1/factor-metrics/results?factor_name=sample&calc_batch_id=batch1"
    "&eval_window=full&expected_snapshot_date=2026-08-31&expected_universe=pit_v2"
    "&expected_return_horizon=1d&limit=1"
)


def _metrics_payload() -> dict[str, Any]:
    return {"ok": True, "domain": "factor_metrics.result", "summary_first": True, "total": 1,
            "items": [{"id": 1, "factor_name": "sample", "calc_batch_id": "batch1", "eval_window": "full",
                       "snapshot_date": "2026-08-31", "universe": "pit_v2", "return_horizon": "1d",
                       "coverage": .9, "n_trading_days": 100, "ic_mean": -.1, "rank_ic_mean": -.2,
                       "icir": -2., "rank_icir": -3., "ic_positive_ratio": .4,
                       "calculated_at": "2026-09-30T13:04:21+08:00"}],
            "pagination": {"limit": 1, "offset": 0, "next_offset": 1, "total": 1, "has_more": False}}


def test_factor_metrics_semantics_bind_real_results_without_profitability_threshold() -> None:
    _, verdict = workflow._evaluate_business_smoke_semantics(
        _METRICS_PROBE, json.dumps(_metrics_payload()), response_sha256="a" * 64
    )
    assert verdict["verdict"] == "passed" and verdict["contract_id"] == "factor_metrics_results"
    assert verdict["facts"]["calc_batch_id"] == "batch1"
    assert verdict["facts"]["acceptance_scope"] == "bound_metrics_readback_only"
    assert verdict["facts"]["offline_algorithm_acceptance"] == "requires_separate_bug_specific_evidence"


@pytest.mark.parametrize("query", [
    "limit=1", "", "factor_name=sample&limit=1",
    _METRICS_PROBE.split("?", 1)[1] + "&calc_batch_id=other",
    _METRICS_PROBE.split("?", 1)[1].replace("batch1", "other"),
    _METRICS_PROBE.split("?", 1)[1].replace("2026-08-31", "2026-8-31"),
    _METRICS_PROBE.split("?", 1)[1].replace("limit=1", "limit=0"),
    _METRICS_PROBE.split("?", 1)[1].replace("limit=1", "limit=" + "9" * 5000),
    _METRICS_PROBE.split("?", 1)[1] + "&offset=1",
])
def test_factor_metrics_semantics_reject_unbound_or_conflicting_probe(query: str) -> None:
    assert workflow._validate_factor_metrics_results(
        _metrics_payload(), url=_METRICS_PROBE.split("?", 1)[0] + "?" + query
    )[0] == "failed"


@pytest.mark.parametrize(("field", "value"), [
    ("factor_name", "other"), ("calc_batch_id", "old_batch"), ("eval_window", "2024"),
    ("snapshot_date", "2026-06-30"), ("universe", "old_pool"), ("return_horizon", "20d"),
    ("id", True), ("n_trading_days", 0), ("coverage", float("nan")), ("ic_mean", float("inf")),
    ("rank_ic_mean", 2), ("ic_positive_ratio", -1), ("icir", None), ("rank_icir", "1"),
    ("h20_ic_mean", float("nan")), ("ic_mean", 10 ** 500),
    ("calculated_at", "2026-09-30T13:04:21"), ("calculated_at", "2026-08-30T13:04:21+08:00"),
])
def test_factor_metrics_semantics_reject_invalid_business_rows(field: str, value: Any) -> None:
    payload = _metrics_payload()
    payload["items"][0][field] = value
    assert workflow._validate_factor_metrics_results(payload, url=_METRICS_PROBE)[0] == "failed"


@pytest.mark.parametrize(("field", "value"), [
    ("items", []), ("total", True), ("total", 0), ("ok", False), ("domain", "other"),
    ("errors", ["failed"]), ("pagination", {}),
    ("success", False), ("status", "failed"),
    ("pagination", {"limit": 1, "offset": 0, "next_offset": 1, "total": 2, "has_more": False}),
])
def test_factor_metrics_semantics_reject_empty_or_contradictory_envelope(field: str, value: Any) -> None:
    payload = _metrics_payload()
    payload[field] = value
    assert workflow._validate_factor_metrics_results(payload, url=_METRICS_PROBE)[0] == "failed"


def test_factor_metrics_semantics_reject_duplicate_persisted_rows() -> None:
    payload = _metrics_payload()
    payload["items"] *= 2
    payload["total"] = 2
    payload["pagination"].update(limit=2, next_offset=2, total=2)
    assert workflow._validate_factor_metrics_results(payload, url=_METRICS_PROBE.replace("limit=1", "limit=2"))[0] == "failed"


def _business_semantic(url: str, payload: Any, response_hash: str) -> dict[str, Any]:
    return workflow._evaluate_business_smoke_semantics(
        url, json.dumps(payload), response_sha256=response_hash,
    )[1]


def _entry_price_status_semantic(payload: Any, *, program_id: str = "advp_test") -> dict[str, Any]:
    return _business_semantic(
        f"http://127.0.0.1:8001/api/v1/advisory/programs/{program_id}/entry-price/status", payload, "e" * 64,
    )


def _entry_price_payload(configured=False):
    return {
        "ok": True,
        "schema_version": "advisory_entry_price_status_v1",
        "configured": configured,
        "program_id": "advp_test",
        "status": "QUALITY_REVIEW_REQUIRED" if configured else "NOT_CONFIGURED",
        "database_written": False,
        **({"binding_activated": False} if configured else {}),
    }


@pytest.mark.parametrize("configured", [False, True])
def test_entry_price_status_semantic_contract_accepts_safe_readback(configured) -> None:
    payload = _entry_price_payload(configured)
    semantic = _entry_price_status_semantic(payload)
    assert semantic["contract_id"] == "advisory_entry_price_status"
    assert semantic["verdict"] == "passed"
    assert semantic["facts"] == {key: payload[key] for key in (
        "program_id", "configured", "status", "database_written"
    )}


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"ok": False}, "ok=true"),
        ({"errors": ["readback failed"]}, "without errors"),
        ({"schema_version": "wrong"}, "schema_version"),
        ({"program_id": "advp_other"}, "does not match"),
        ({"configured": "false"}, "configured must be boolean"),
        ({"status": "CONFIGURED"}, "must be NOT_CONFIGURED"),
        ({"database_written": True}, "database_written=false"),
        (
            {"configured": True, "status": "CONFIGURED", "binding_activated": True},
            "binding_activated=false",
        ),
    ],
)
def test_entry_price_status_semantic_contract_rejects_invalid_or_unsafe_readback(
    override: dict[str, Any],
    reason: str,
) -> None:
    payload = _entry_price_payload()
    payload.update(override)

    semantic = _entry_price_status_semantic(payload)

    assert semantic["verdict"] == "failed"
    assert reason in semantic["reason"]


def _rotation_l2_overview_payload(run_id: str = "a" * 64) -> dict[str, Any]:
    return {
        "status": "ok",
        "data": {
            "run_id": run_id,
            "model_hash": "b" * 64,
            "trade_date": "2026-09-26",
            "as_of_date": "2026-09-25",
            "sector_count": 131,
            "available_count": 119,
            "canonical_row_sha256": "c" * 64,
        },
    }


def _rotation_l2_semantic(
    payload: Any,
    *,
    query: str = "run_id=" + "a" * 64,
) -> dict[str, Any]:
    return _business_semantic(
        f"http://127.0.0.1:8001/api/v1/hmm-risk/rotation-l2/overview?{query}",
        payload, "d" * 64,
    )


def test_rotation_l2_overview_semantic_contract_binds_complete_run() -> None:
    semantic = _rotation_l2_semantic(_rotation_l2_overview_payload())

    assert semantic["contract_id"] == "hmm_rotation_l2_overview"
    assert semantic["verdict"] == "passed"
    assert semantic["facts"] == {
        "run_id": "a" * 64,
        "model_hash": "b" * 64,
        "canonical_row_sha256": "c" * 64,
        "trade_date": "2026-09-26",
        "as_of_date": "2026-09-25",
        "sector_count": 131,
        "available_count": 119,
    }


_ROTATION_RUN_QUERY = "run_id=" + "a" * 64


@pytest.mark.parametrize("query,envelope,fields,reason", [
    (_ROTATION_RUN_QUERY, {"status": "failed"}, {}, "status=ok"),
    (_ROTATION_RUN_QUERY, {"ok": False}, {}, "status=ok"),
    (_ROTATION_RUN_QUERY, {"errors": ["readback failed"]}, {}, "status=ok"),
    ("", {}, {}, "exactly one non-empty run_id"),
    (_ROTATION_RUN_QUERY + "&" + _ROTATION_RUN_QUERY, {}, {}, "exactly one non-empty run_id"),
    ("run_id=" + "A" * 64, {}, {"run_id": "A" * 64}, "lowercase SHA-256"),
    (_ROTATION_RUN_QUERY, {}, {"run_id": "e" * 64}, "does not match"),
    (_ROTATION_RUN_QUERY, {}, {"sector_count": 130}, "complete 131-sector catalog"),
    (_ROTATION_RUN_QUERY, {}, {"sector_count": True}, "complete 131-sector catalog"),
    (_ROTATION_RUN_QUERY, {}, {"available_count": 132}, "outside the sector catalog"),
    (_ROTATION_RUN_QUERY, {}, {"canonical_row_sha256": None}, "lowercase SHA-256"),
    (_ROTATION_RUN_QUERY, {}, {"trade_date": "2026-09-31"}, "ISO date"),
    (_ROTATION_RUN_QUERY, {}, {"trade_date": "2026-9-26"}, "ISO date"),
    (_ROTATION_RUN_QUERY, {}, {"as_of_date": "2026-09-26"}, "must precede"),
])
def test_rotation_l2_overview_semantic_contract_rejects_invalid_readback(query, envelope, fields, reason):
    """One negative oracle, preserving every envelope/query/row case."""
    payload = _rotation_l2_overview_payload()
    payload.update(envelope)
    payload["data"].update(fields)
    semantic = _rotation_l2_semantic(payload, query=query)
    assert semantic["verdict"] == "failed"
    assert reason in semantic["reason"]


def test_unknown_business_smoke_endpoint_remains_fail_closed() -> None:
    semantic = _business_semantic(
        "http://127.0.0.1:8001/api/v1/hmm-risk/rotation-l2/not-registered",
        {"status": "ok", "data": {}}, "f" * 64,
    )

    assert semantic["contract_id"] is None
    assert semantic["verdict"] == "failed"
    assert "no target-owned business-smoke semantic contract" in semantic["reason"]


def test_ci_issue_classification_ignores_successful_runner_and_no_network_metadata() -> None:
    summary = {
        "diagnostic_status": "complete",
        "failed_jobs": [
            {
                "error_signature": "Nightly failed sessions: validation_center_backend",
                "key_log_excerpt": [
                    "runner_preflight: success",
                    "FAILED backend/tests/scripts/test_issue_flow.py::test_validation_select",
                ],
            }
        ],
    }
    issue = {
        "title": "[P1][validation_center] Nightly failed: validation_center_backend",
        "body": """## Nightly Statuses

- runner_preflight: `success`
- nightly_l3: `failure`

## LLM Triage Advice

- reason: `schema_quality_smoke_no_network`

## Agent Handoff

Restore the self-hosted runner when infrastructure fails.
""",
    }

    assert workflow._classify_ci_issue(summary, issue) == "real_regression_candidate"


@pytest.mark.parametrize(
    ("title", "error_signature"),
    [
        ("P1 Nightly blocked: self-hosted Windows runner unavailable", "Nightly failed"),
        ("P1 Nightly failed", "no online GitHub Actions runner matches required labels"),
        ("P1 Nightly failed", "runner_preflight=failure"),
    ],
)
def test_ci_issue_classification_keeps_explicit_runner_failures_infrastructure(
    title: str,
    error_signature: str,
) -> None:
    summary = {
        "diagnostic_status": "complete",
        "failed_jobs": [{"error_signature": error_signature, "key_log_excerpt": []}],
    }

    assert workflow._classify_ci_issue(summary, {"title": title, "body": ""}) == "infra_blocker"


def test_merge_uses_stable_quality_contract_and_ignores_advisory_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        if args[:3] == ["gh", "pr", "view"]:
            return _result(
                stdout=json.dumps(
                    {
                        "state": "OPEN",
                        "statusCheckRollup": [
                            {"name": "CI verdict", "status": "COMPLETED", "conclusion": "SUCCESS"},
                            {"name": "advisory", "status": "COMPLETED", "conclusion": "FAILURE"},
                        ],
                    }
                )
            )
        if args[:3] == ["gh", "pr", "checks"]:
            return _result(
                stdout=json.dumps(
                    [
                        {"name": name, "state": "SUCCESS", "bucket": "pass", "workflow": "quality"}
                        for name in workflow.MERGE_QUALITY_CHECK_CONTEXTS
                    ]
                    + [{"name": "advisory", "state": "FAILURE", "bucket": "fail", "workflow": "advisory"}]
                )
            )
        if args[:3] == ["gh", "pr", "merge"]:
            return _result()
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {"checked": True, "merged": True, "pr": {"mergeCommit": {"oid": "merge123"}}},
    )

    payload = workflow._merge_pr_if_ready("https://github.example/pull/199")

    assert payload["check_summary"]["passed"] == list(workflow.MERGE_QUALITY_CHECK_CONTEXTS)
    assert payload["check_summary"]["failed"] == []
    assert any(args[:3] == ["gh", "pr", "merge"] for args in commands)


def test_required_check_unknown_bucket_fails_closed() -> None:
    summary = workflow._required_pr_check_summary(
        _result(stdout=json.dumps([{"name": "CI verdict", "bucket": "mystery"}]))
    )

    assert summary["failed"] == ["CI verdict"]
    assert summary["passed"] == []


@pytest.mark.parametrize(
    ("changed_file", "expected_impact", "expected_targets"),
    [
        ("backend/main.py", "backend", ["backend-main"]),
        ("backend/routers/monthly_dataset_releases.py", "worker_scheduler", ["worker-scheduler"]),
        ("backend/services/dataset_release/monthly_repair_inputs.py", "worker_scheduler", ["worker-scheduler"]),
        ("backend/services/dataset_release/monthly_postgres_source.py", "worker_scheduler", ["worker-scheduler"]),
        ("backend/routers/quantevolver.py", "backend", ["backend-main"]),
        (
            "backend/services/dataset_release/index_contract.py",
            "worker_scheduler",
            ["worker-scheduler"],
        ),
        ("backend/services/hmm_risk/rotation_l1_gbdt.py", "none", []),
        ("scripts/aistock_runner_health.py", "none", []),
    ],
)
def test_repository_runtime_catalog_preserves_representative_roles(
    changed_file: str,
    expected_impact: str,
    expected_targets: list[str],
) -> None:
    payload = workflow._classify_runtime_impact([changed_file])

    assert payload["runtime_impact"] == expected_impact
    assert payload["target_ids"] == expected_targets


@pytest.mark.parametrize("monthly", [True, False, "construction"])
def test_release_sources_select_their_own_process_probe(monthly) -> None:
    catalog = workflow._load_runtime_target_catalog()
    target = catalog["targets"]["worker-scheduler"]
    monthly_sources = [
        "backend/services/dataset_release/artifact_ready_build_source.py",
        "backend/services/dataset_release/build_stage.py",
        "backend/services/dataset_release/candidate_validator.py",
        "backend/services/dataset_release/factor_materializer.py",
        "backend/services/dataset_release/monthly_consumer_layout.py",
        "backend/services/dataset_release/monthly_local_validation.py",
        "backend/services/dataset_release/monthly_worker_nodes.py",
        "backend/services/dataset_release/monthly_worker_runtime.py",
    ]
    if monthly == "construction":
        monthly_sources = ["backend/services/dataset_release/monthly_construction_facts.py"]

    selected, error = workflow._select_runtime_probe_route(
        target, runtime_files=monthly_sources if monthly else ["backend/services/dataset_release/build_stage.py"],
    )

    assert error is None
    if monthly:
        assert selected["probe_route_id"] == "monthly_release_worker_process"
        assert selected["probe_mode"] == workflow._MONTHLY_RELEASE_WORKER_PROCESS_MODE
        assert selected["probes"] == workflow._MONTHLY_RELEASE_WORKER_PROCESS_REFS
        assert selected["probe_origins"] == ["http://127.0.0.1:8001", "http://localhost:8001"]
    else:
        assert selected["probe_route_id"] == "dataset_release_worker_heartbeat"
        assert selected["probe_mode"] == workflow._DATASET_RELEASE_WORKER_HEARTBEAT_MODE


@pytest.mark.parametrize("worker_count", [1, 2])
def test_monthly_release_process_probes_bind_exact_worker_count(monkeypatch, worker_count) -> None:
    target = {
        "probe_origins": ["http://127.0.0.1:8001"],
        "probes": workflow._MONTHLY_RELEASE_WORKER_PROCESS_REFS,
    }
    snapshot = {
        "schema_version": "aistock_monthly_release_worker_process_snapshot_v1",
        "backend_listener_count": 1,
        "worker_count": worker_count,
        "healthy": worker_count == 1,
        "workers": [{"pid": 101 + index, "ppid": 100} for index in range(worker_count)],
    }
    monkeypatch.setattr(workflow, "_monthly_release_worker_process_snapshot", lambda _target: snapshot)
    monkeypatch.setattr(
        workflow,
        "_read_only_http_probe",
        lambda name, url, **_kwargs: {
            "name": name,
            "url": url,
            "status": "passed",
            "_response_body": json.dumps({"commit": "a" * 40}),
        },
    )
    monkeypatch.setattr(
        workflow,
        "_read_dataset_release_worker_heartbeat_probes",
        lambda *_args, **_kwargs: pytest.fail("monthly process probe must not read generic heartbeat"),
    )

    results = workflow._read_monthly_release_worker_process_probes(target, 3.0)

    assert [item["name"] for item in results] == ["health_ref", "identity_ref", "business_smoke_ref"]
    assert results[2]["semantic"]["contract_id"] == "monthly_release_worker_supervision"
    if worker_count == 1:
        assert all(item["status"] == "passed" for item in results)
    else:
        assert results[0]["status"] == "failed"
        assert results[2]["status"] == "failed"
        assert results[2]["semantic"]["verdict"] == "failed"
        assert "worker_count=2" in results[2]["error"]


def test_monthly_release_process_snapshot_binds_worker_to_backend_listener(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_port = 8_000 + 1
    worker_path = tmp_path / "scripts" / "monthly_unified_dataset_release_worker.py"
    worker_path.parent.mkdir(parents=True)
    worker_path.write_text("# worker\n", encoding="utf-8")

    child = SimpleNamespace(
        pid=102, ppid=lambda: 101, cwd=lambda: str(tmp_path),
        cmdline=lambda: ["python", str(worker_path), "--serve", "--poll-seconds", "5.0"],
    )
    def children(*, recursive):
        assert recursive is False
        return [child]
    parent = SimpleNamespace(
        pid=101, cwd=child.cwd, children=children,
        cmdline=lambda: ["python", "-m", "uvicorn", "backend.main:app", "--port", str(parent_port)],
    )

    monkeypatch.setattr(workflow, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        workflow.psutil,
        "net_connections",
        lambda **_kwargs: [
            SimpleNamespace(
                pid=101,
                status=workflow.psutil.CONN_LISTEN,
                laddr=SimpleNamespace(port=parent_port),
            )
        ],
    )
    monkeypatch.setattr(workflow.psutil, "Process", lambda pid: parent if pid == 101 else pytest.fail())

    snapshot = workflow._monthly_release_worker_process_snapshot(
        {
            "local_probe": {
                "worker_script": "scripts/monthly_unified_dataset_release_worker.py",
                "worker_mode": "--serve",
                "parent_module": "backend.main:app",
                "parent_port": parent_port,
            }
        }
    )

    assert snapshot["backend_listener_count"] == 1
    assert snapshot["worker_count"] == 1
    assert snapshot["healthy"] is True


def test_repository_runtime_catalog_omits_retired_hmm_sources() -> None:
    catalog = workflow._load_runtime_target_catalog()

    retired = {
        "backend/services/hmm_risk/b3_d1_inactive_dimension.py",
        "backend/services/hmm_risk/b3_mixed_dimension.py",
        "backend/services/hmm_risk/b3_training.py",
        "backend/services/hmm_risk/state_model_set.py",
    }

    assert retired.isdisjoint(catalog["non_runtime_source_paths"])


@pytest.fixture
def invalid_runtime_catalog(monkeypatch: pytest.MonkeyPatch) -> str:
    message = "runtime target catalog contains one stale source"

    def fail_catalog(_root: Path | None = None) -> dict[str, Any]:
        raise workflow.WorkflowError(message)

    monkeypatch.setattr(workflow, "_load_runtime_target_catalog", fail_catalog)
    return message


def test_runtime_classifier_surfaces_catalog_validation_error(invalid_runtime_catalog: str) -> None:

    payload = workflow._classify_runtime_impact(
        ["backend/services/hmm_risk/contracts.py"]
    )

    assert payload["runtime_impact"] == "unknown"
    assert payload["target_ids"] == ["backend-main"]
    assert payload["catalog_error"] == invalid_runtime_catalog


def test_runtime_contract_blocks_on_catalog_validation_error(invalid_runtime_catalog: str) -> None:
    contract = workflow.build_runtime_contract(
        record={
            "runtime_contract": {
                "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
                "runtime_impact": "none",
                "target_ids": [],
            }
        },
        changed_files=["backend/services/hmm_risk/contracts.py"],
    )

    assert contract["runtime_impact"] == "unknown"
    assert contract["catalog_validation_error"] == invalid_runtime_catalog
    assert f"runtime target catalog validation failed: {invalid_runtime_catalog}" in contract["blocking"]
    assert contract["pre_pr_ready"] is False


@pytest.mark.parametrize("path,registered", [
    ("/api/v1/audit-unregistered-contract", False),
    ("/api/v1/factor-metrics/results", True),
])
def test_runtime_preflight_checks_semantic_registration_without_http(path, registered) -> None:
    root = workflow.REPO_ROOT
    runbook = next((root / "docs/operations").glob("*.md")).relative_to(root).as_posix()
    contract = workflow.build_runtime_contract(
        record={"runtime_contract": {
            "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
            "operator_runbook_ref": runbook,
            "identity_ref": "http://127.0.0.1:8001/api/v1/runtime/identity",
            "business_smoke_ref": "http://127.0.0.1:8001" + path,
            "fresh_process_evidence": ["synthetic-contract-presence-only"],
        }},
        changed_files=["backend/main.py"],
    )
    semantic_errors = [error for error in contract["blocking"] if "semantic contract" in error]
    assert bool(semantic_errors) is not registered
    assert contract["pre_pr_ready"] is registered


def test_bug_1549_active_contract_consumers_remain_backend_main() -> None:
    payload = workflow._classify_runtime_impact(
        [
            "backend/services/hmm_risk/b3_d1_inactive_dimension.py",
            "backend/services/hmm_risk/b3_mixed_dimension.py",
            "backend/services/hmm_risk/b3_training.py",
            "backend/services/hmm_risk/contracts.py",
            "backend/services/hmm_risk/risk_l1_prediction.py",
            "backend/services/hmm_risk/state_model_set.py",
            "backend/services/hmm_risk/stock_fact_observation.py",
        ]
    )

    assert payload["runtime_impact"] == "backend"
    assert payload["target_ids"] == ["backend-main"]
    assert payload["catalog_error"] is None


def test_find_bug_record_parses_only_matching_or_opaque_filenames(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    matching = tmp_path / "20260813_BUG-199-example.json"
    unrelated = tmp_path / "20260813_BUG-200-example.json"
    opaque = tmp_path / "legacy.json"
    matching.write_text(json.dumps({"bug_id": "BUG-199"}), encoding="utf-8")
    unrelated.write_text(json.dumps({"bug_id": "BUG-200"}), encoding="utf-8")
    opaque.write_text(json.dumps({"bug_id": "BUG-201"}), encoding="utf-8")
    loaded: list[Path] = []
    original = workflow._load_json

    def recording_load(path: Path) -> dict[str, Any]:
        loaded.append(path)
        return original(path)

    monkeypatch.setattr(workflow, "_bug_files", lambda: [matching, unrelated, opaque])
    monkeypatch.setattr(workflow, "_load_json", recording_load)

    record, path = workflow.find_bug_record("BUG-199")

    assert record["bug_id"] == "BUG-199"
    assert path == matching
    assert loaded == [matching, opaque]


def test_compact_terminal_reservation_is_exact_and_keeps_other_records(tmp_path: Path) -> None:
    target = tmp_path / "BUG-199.json"
    other = tmp_path / "BUG-200.json"
    target.write_text(json.dumps({"bug_id": "BUG-199", "status": "registered"}), encoding="utf-8")
    other.write_text(json.dumps({"bug_id": "BUG-200", "status": "registered"}), encoding="utf-8")

    removed = compact_terminal_reservation(tmp_path, "BUG-199", min_age_seconds=0)

    assert removed == str(target)
    assert not target.exists()
    assert other.exists()


def test_read_command_retry_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return _result(ok=calls == 3, stderr="TLS EOF" if calls < 3 else "", returncode=0 if calls == 3 else 1)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow.time, "sleep", lambda seconds: None)

    result = workflow._run_read_command_with_retry(["gh", "pr", "view", "1"], attempts=3)

    assert result["ok"] is True
    assert result["attempts"] == 3
    assert calls == 3


@pytest.mark.parametrize("module", ["validation.workflow_automation", "unknown"])
def test_github_issue_create_only_recovers_nested_module_labels(tmp_path, monkeypatch, module) -> None:
    commands: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        if len(commands) == 1:
            return _result(
                ok=False,
                stderr=f"could not add label: 'module:{module}' not found",
                returncode=1,
            )
        return _result(stdout="https://github.com/licong01-cloud/AIstock/issues/4601\n")

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    body = tmp_path / "body.md"
    body.write_text("issue", encoding="utf-8")

    def create():
        return workflow._create_github_issue_with_recovery(
            bug_id="BUG-1461", title="BUG-1461 P2: example", body_path=body,
            labels=["aistock:bug", f"module:{module}", "status:open"], cwd=tmp_path,
        )
    if module == "unknown":
        with pytest.raises(workflow.WorkflowError, match="module:unknown"):
            create()
        assert len(commands) == 1
        return
    result = create()

    assert result["number"] == 4601
    assert result["warnings"] == [
        "GitHub label module:validation.workflow_automation was unavailable; used module:validation"
    ]
    assert len(commands) == 2
    assert "module:validation.workflow_automation" in commands[0][-1]
    assert "module:validation" in commands[1][-1]
    assert "module:validation.workflow_automation" not in commands[1][-1]


def test_workflow_smoke_does_not_call_full_doctor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    issue = tmp_path / "bug.json"
    issue.write_text(json.dumps({"bug_id": "BUG-199"}), encoding="utf-8")
    monkeypatch.setattr(workflow, "build_doctor_report", lambda **kwargs: pytest.fail("full doctor called"))
    monkeypatch.setattr(workflow, "_git_status_paths", lambda root: [])
    monkeypatch.setattr(workflow, "build_fast_path_plan", lambda **kwargs: {"workflow_gate": "planned"})
    monkeypatch.setattr(workflow, "build_start_plan", lambda **kwargs: {"bug_id": "BUG-199"})
    monkeypatch.setattr(
        workflow,
        "build_finish_plan",
        lambda **kwargs: {"workflow_gate": "plan_ready", "artifact_metrics": {}},
    )
    monkeypatch.setattr(workflow, "_workflow_timing_summary", lambda *args, **kwargs: {"event_count": 0})

    payload = workflow.build_workflow_smoke_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        module="validation",
    )

    assert payload["workflow_gate"] == "passed"
    assert payload["client_manifest"] is None


def test_runtime_pending_close_sync_does_not_create_intermediate_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted: dict[str, Any] = {}
    monkeypatch.setattr(
        workflow,
        "build_close_sync_plan",
        lambda **kwargs: {"bug_id": "BUG-199", "workflow_gate": "fixed_source_pending_user_restart"},
    )
    monkeypatch.setattr(
        workflow,
        "_maybe_commit_and_pr_close_sync",
        lambda **kwargs: pytest.fail("intermediate close-sync PR created"),
    )
    monkeypatch.setattr(workflow, "_production_gates_payload", lambda args=None: {})
    monkeypatch.setattr(workflow, "_emit_args", lambda payload, args: emitted.update(payload))
    args = argparse.Namespace(
        bug_id="BUG-199",
        issue_json=None,
        pr_url="https://github.example/pull/199",
        apply=True,
        allow_missing_linkage=False,
        validation_evidence=["pytest -> passed"],
        merge_commit="a" * 40,
        skip_github_check=False,
        create_registry_worktree=True,
        allow_current_worktree=False,
        post_restart_receipt=None,
        create_pr=True,
    )

    assert workflow.cmd_close_sync(args) == 0
    assert emitted["close_sync_commit"]["workflow_gate"] == "deferred_runtime_verification"


@pytest.mark.parametrize("mode", ["merged", "planned", "invalid"])
def test_post_restart_scope_comes_from_recorded_merge_not_planned_paths(cli_task_worktree, monkeypatch, mode):
    base, task, git = cli_task_worktree
    record = {"bug_id": "BUG-199", "file_scope_contract": {"changed_files": ["backend/services/dataset_release/build_stage.py"]}}
    if mode != "planned":
        record["fix_commit"] = git(task, "rev-parse", "HEAD") if mode == "merged" else "a" * 40
    monkeypatch.setattr(workflow, "find_bug_record", lambda **kwargs: (record, base / "BUG-199.json"))
    def capture(**kwargs):
        expected = ["task.txt"] if mode == "merged" else record["file_scope_contract"]["changed_files"]
        assert kwargs["changed_files"] == expected
        raise RuntimeError("scope captured before probes")
    monkeypatch.setattr(workflow, "build_runtime_contract", capture)
    # An operator identity override is not authority to replace the source delta.
    with pytest.raises(workflow.WorkflowError if mode == "invalid" else RuntimeError,
                       match=None if mode == "invalid" else "scope captured"):
        workflow.build_post_restart_verify(bug_id="BUG-199", issue_json=None,
                                           target_id="backend-main", expected_identity="b" * 40)


@pytest.mark.parametrize("pr_number,commit,accepted,recovery_number,guard_accepted", [
    (199, "a", True, 199, True), (200, "a", False, 199, True),
    (199, "b", False, 199, True), (199, "a", True, 200, False),
])
def test_recoverable_close_sync_dirty_record_requires_exact_source_identity(
    tmp_path, monkeypatch, pr_number, commit, accepted, recovery_number, guard_accepted,
) -> None:
    dirty_path = "tests/aistock_validation/bugs/BUG-199.json"
    issue = tmp_path / dirty_path
    issue.parent.mkdir(parents=True)
    record = {"bug_id": "BUG-199", "status": "fixed", "fix_commit": "a" * 40,
              "pr_url": "https://github.example/pull/199"}
    issue.write_text(json.dumps(record), encoding="utf-8")
    monkeypatch.setattr(workflow, "_dirty_files", lambda _root: [dirty_path])
    recovered = workflow._recoverable_close_sync_dirty_record(
        tmp_path, "BUG-199", issue, source_pr_url=f"https://github.example/pull/{pr_number}", merge_commit=commit * 40,
    )
    if accepted:
        assert recovered is not None and recovered["path"] == dirty_path
    else:
        assert recovered is None
    monkeypatch.setattr(
        workflow, "_validate_registry_apply_target", lambda _root: {
            "blocking": ["registry target is dirty (1 file(s)); start from a clean task worktree"],
            "warnings": [],
            "git": {"dirty": True, "dirty_count": 1},
        },
    )
    recovery = {**record, "path": f"tests/aistock_validation/bugs/BUG-{recovery_number}.json"}
    result = workflow._validate_close_sync_apply_target(tmp_path, recoverable_dirty_record=recovery)
    if guard_accepted:
        assert result["blocking"] == []
        assert result["recoverable_dirty_record"] == recovery
    else:
        assert result["blocking"] == ["registry target is dirty (1 file(s)); start from a clean task worktree"]


def test_windows_process_scan_builds_full_caller_ancestor_exclusion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(workflow.os, "name", "nt")

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["args"] = args
        captured["env"] = kwargs["env"]
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="[]", stderr="")

    monkeypatch.setattr(workflow.shutil, "which", lambda name: "powershell.exe")
    monkeypatch.setattr(workflow.subprocess, "run", fake_run)

    profile = workflow._worktree_active_process_profile(tmp_path)

    assert profile["reference_count"] == 0
    assert "AISTOCK_CLEANUP_CALLER_PID" in captured["env"]
    assert "ParentProcessId" in captured["args"][-1]
    assert "AISTOCK_CLEANUP_EXCLUDE_PIDS" not in captured["env"]


@pytest.mark.parametrize("valid_format", [True, False])
def test_backend_lifespan_logs_are_transient_only_for_exact_bounded_format(tmp_path, valid_format) -> None:
    log_root = tmp_path / "backend" / "logs"
    log_root.mkdir(parents=True)
    (log_root / "aistock.log").write_text(
        "2026-08-13 03:10:31 INFO [backend.main] lifespan validation started\n"
        if valid_format else "Traceback: retain this evidence\n",
        encoding="utf-8",
    )
    paths = ["backend/logs/aistock.log"]
    if valid_format:
        (log_root / "errors.log").write_text("", encoding="utf-8")
        paths.append("backend/logs/errors.log")

    accepted, reason = workflow._validated_backend_lifespan_log_transient_paths(
        paths,
        worktree_path=tmp_path,
    )

    if not valid_format:
        assert accepted == set()
        assert reason == "backend_lifespan_log_format_mismatch"
        return
    assert accepted == set(paths)
    assert reason == "bounded_test_created_backend_lifespan_log"

    (log_root / "aistock.log.1").write_text("rotated evidence", encoding="utf-8")
    rejected, reject_reason = workflow._validated_backend_lifespan_log_transient_paths(
        ["backend/logs/aistock.log", "backend/logs/errors.log", "backend/logs/aistock.log.1"],
        worktree_path=tmp_path,
    )

    assert rejected == set()
    assert reject_reason == "backend_lifespan_log_inventory_mismatch"


def test_cleanup_discovers_registered_worktree_when_argument_is_omitted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    task_worktree = tmp_path / "task-worktree"
    task_worktree.mkdir()

    def fake_git(args: list[str], **kwargs: Any) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "main"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        return ""

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        if args[:2] == ["git", "status"]:
            return _result()
        if args[:2] == ["git", "ls-files"]:
            return _result(stdout="")
        if args[:3] == ["git", "ls-remote", "--heads"]:
            return _result(stdout="")
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_canonical_root", lambda: tmp_path)
    monkeypatch.setattr(workflow, "_registered_worktree_for_branch", lambda value, cwd=None: task_worktree)
    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow, "_path_is_registered_worktree", lambda path, cwd=None: True)
    monkeypatch.setattr(workflow, "_git_snapshot", lambda root: {"branch": "main", "dirty": False})
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: [])
    monkeypatch.setattr(workflow, "_cleanup_protected_receipt_paths", lambda bug_id: set())
    monkeypatch.setattr(
        workflow,
        "_cleanup_evidence_finalization",
        lambda bug_id: {"durable_receipt_present": True, "status": "finalized_structured_receipt"},
    )

    payload = workflow.build_cleanup_after_merge_plan(branch=branch, apply=False)

    assert payload["workflow_gate"] == "ready_for_cleanup"
    assert payload["worktree"] == str(task_worktree)
    assert payload["worktree_registered"] is True


@pytest.mark.parametrize("cache_matches", [True, False])
def test_cleanup_merged_pr_cache_is_reused_only_for_exact_identity(tmp_path, monkeypatch, cache_matches) -> None:
    branch = "bug/BUG-199-already-partially-cleaned"
    pr_url = "https://github.example/pull/199"
    head = "a" * 40
    merge_commit = "b" * 40
    verified = {
        "checked": True,
        "merged": True,
        "pr": {
            "url": pr_url,
            "headRefName": branch,
            "headRefOid": head,
            "mergeCommit": {"oid": merge_commit},
        },
    }
    readbacks = []
    def readback(value):
        assert cache_matches is False, "cached PR check was reread"
        assert value == pr_url
        readbacks.append(value)
        return verified
    monkeypatch.setattr(workflow, "_verify_pr_merged", readback)
    monkeypatch.setattr(
        workflow,
        "_git_commit_is_ancestor",
        lambda ancestor, descendant, root: ancestor == head and descendant == merge_commit,
    )

    result = workflow._cleanup_merge_verification(
        branch,
        pr_url,
        False,
        cwd=tmp_path,
        verified_pr_check=verified if cache_matches else {
            "checked": True, "merged": True,
            "pr": {
                "url": "https://github.example/pull/200", "headRefName": "bug/BUG-200-other",
                "headRefOid": "c" * 40, "mergeCommit": {"oid": "d" * 40},
            },
        },
    )

    assert result["verified"] is True
    assert result["method"] == "merged_pr_head_is_ancestor_of_merge_commit"
    assert result["tree_equivalence_ref"] == head
    assert len(readbacks) == (0 if cache_matches else 1)


def test_cleanup_preflight_reuses_successful_same_finalizer_fetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cached = {"status": "fetched", "command": "git fetch origin --prune", "result": _result()}
    monkeypatch.setattr(
        workflow,
        "_cleanup_preflight_fetch_origin",
        lambda root, apply: pytest.fail("successful cached fetch was repeated"),
    )

    result = workflow._cleanup_preflight_fetch_for_plan(tmp_path, apply=True, cached=cached)

    assert result["status"] == "fetched"
    assert result["reused"] is True


@pytest.mark.parametrize("stale", [False, True])
def test_merge_aftercare_publishes_changed_and_existing_stale_client_lanes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stale: bool,
) -> None:
    events: list[str] = []
    monkeypatch.setattr(workflow, "_canonical_root", lambda: tmp_path)
    monkeypatch.setattr(
        workflow,
        "_cleanup_preflight_fetch_origin",
        lambda root, apply: {"status": "fetched", "result": _result()},
    )
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"branch": "main", "dirty": False, "head": "same" if stale else "old", "origin_main": "same" if stale else "new"},
    )
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda args, **kwargs: events.append(
            "root_sync" if args[:2] == ["git", "merge"] else "merge_containment"
        )
        or _result(),
    )
    monkeypatch.setattr(
        workflow,
        "_merge_commit_changed_files",
        lambda merge_commit, root: {
            "ok": True,
            "files": [".codex/skills/aistock-merge-aftercare/SKILL.md"] if stale else [
                ".codex/skills/aistock-merge-aftercare/SKILL.md", ".claude/commands/aistock-task-router.md",
                "docs/standards/README.md"],
        },
    )

    def fake_install(*, apply: bool, selected_lane: str, **kwargs: Any) -> dict[str, Any]:
        events.append(f"install:{selected_lane}")
        return {"workflow_gate": "installed", "blocking": []}

    monkeypatch.setattr(workflow, "build_client_install_plan", fake_install)
    monkeypatch.setattr(workflow, "_ClientInstallLock", lambda: workflow.contextlib.nullcontext())
    monkeypatch.setattr(workflow, "_client_manifest", lambda: {
        "codex_entries": {"merge_aftercare": {"status": "current"}, "validation_delegation": {"status": "stale"}},
        "claude_entries": {"validation_delegation": {"status": "stale_global"}, "readonly_triage": {"status": "missing_global"}},
    } if stale else {})
    monkeypatch.setattr(
        workflow,
        "_client_lane_verification",
        lambda manifest, selected_lane, verify_codex, verify_claude: {"ready": True, "blocking": []},
    )

    result = workflow._publish_changed_clients_after_merge(
        merge_commit="c" * 40,
        sync_root=True,
        apply=True,
    )

    assert result["workflow_gate"] == "installed_and_verified"
    if stale:
        assert result["changed_lanes"] == ["merge_aftercare"]
        assert result["stale_lanes_before"] == ["readonly_triage", "validation_delegation"]
        assert [event for event in events if event.startswith("install:")] == [
            "install:merge_aftercare", "install:readonly_triage", "install:validation_delegation"]
    else:
        assert result["selected_lanes"] == ["merge_aftercare", "router"]
        assert events == ["root_sync", "merge_containment", "install:merge_aftercare", "install:router"]
        assert result["merge_commit_containment"]["ok"] is True


@pytest.mark.parametrize("case", ["update", "noop", "foreign", "nonancestor", "drift", "transport", "crlf_update", "crlf_noop", "crlf_transport", "body_change"])
def test_owned_pr_receipt_sync_is_exact_and_recoverable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str) -> None:
    url, branch, old, new = "https://github.com/licong01-cloud/AIstock/pull/1", "bug/task", "a" * 40, "b" * 40
    body = tmp_path / "body.md"
    body.write_text("validated receipts\nexact task identity\n", encoding="utf-8")
    row = {"pr_number": 1, "url": url, "state": "OPEN", "head_ref": branch, "base_ref": "main",
           "head_repo": "foreign/repo" if case == "foreign" else workflow.GITHUB_REPO,
           "head_sha": new if case in {"noop", "crlf_noop"} else old,
           "body": body.read_text().replace("\n", "\r\n") if case == "crlf_noop" else body.read_text() if case == "noop" else "stale"}
    writes: list[str] = []
    monkeypatch.setattr(workflow, "_github_pull_rest_readback", lambda url: dict(row))
    def run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        if args[0] == "git":
            return {"ok": case != "nonancestor"}
        writes.append("PATCH")
        row["body"] = body.read_text()
        if case.startswith("crlf_"):
            row["body"] = row["body"].replace("\n", "\r\n")
        if case == "body_change":
            row["body"] += " "
        if case in {"transport", "crlf_transport"}:
            return {"ok": False, "stderr": "TLS handshake timeout"}
        return {"ok": True, "stdout": json.dumps({"state": "open", "body": row["body"], "number": 1, "html_url": url,
            "head": {"sha": new if case == "drift" else old, "ref": branch, "repo": {"full_name": workflow.GITHUB_REPO}},
            "base": {"ref": "main"}})}
    monkeypatch.setattr(workflow, "_run_command", run)
    if case in {"foreign", "nonancestor", "drift", "body_change"}:
        with pytest.raises(workflow.WorkflowError):
            workflow._sync_owned_pr_body(pr_url=url, branch=branch, body_path=body, expected_head=new, before_push=True)
        assert len(writes) == (1 if case in {"drift", "body_change"} else 0)
    else:
        result = workflow._sync_owned_pr_body(pr_url=url, branch=branch, body_path=body, expected_head=new, before_push=True)
        assert result["body_updated"] == (case not in {"noop", "crlf_noop"})


@pytest.mark.parametrize("same_head", [False, True])
def test_existing_source_pr_receipts_are_published_before_push_and_reused(monkeypatch: pytest.MonkeyPatch, same_head: bool) -> None:
    events: list[str] = []
    url = "https://github.com/licong01-cloud/AIstock/pull/1"
    monkeypatch.setattr(workflow, "_load_state", lambda bug_id: {"pr_url": url})
    monkeypatch.setattr(workflow, "_pr_worktree_guard", lambda: {"blocking": []})
    monkeypatch.setattr(workflow, "_current_branch", lambda: "bug/task")
    monkeypatch.setattr(workflow, "_check_pr_receipt_identity", lambda *args, **kwargs: None)
    monkeypatch.setattr(workflow, "_pre_pr_gate", lambda **kwargs: {"workflow_gate": "passed"})
    monkeypatch.setattr(workflow, "_git", lambda *args, **kwargs: "b" * 40)
    monkeypatch.setattr(workflow, "_run_command", lambda *args, **kwargs: {"ok": True, "stdout": "b" * 40})
    monkeypatch.setattr(workflow, "_sync_owned_pr_body", lambda **kwargs: events.append("receipt_before_push" if kwargs["before_push"] else "head_readback") or {"url": url, "head_sha": ("b" if same_head else "a") * 40})
    monkeypatch.setattr(workflow, "_execute_workflow_command", lambda *args, **kwargs: events.append("push") or {"ok": True})
    monkeypatch.setattr(workflow, "_create_pr_with_transport_fallback", lambda **kwargs: pytest.fail("existing PR recreated"))
    monkeypatch.setattr(workflow, "_write_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(workflow, "_append_event", lambda *args, **kwargs: None)
    result = workflow._maybe_create_pr(bug_id="BUG-999", finish={"validation_evidence": ["passed"], "pr_body_path": "body.md"},
        push=True, create_pr=True, watch_ci=False, pr_title=None)
    assert events == (["receipt_before_push", "head_readback"] if same_head else ["receipt_before_push", "push", "head_readback"])
    assert result["pr_url"] == url


@pytest.mark.skipif(workflow.os.name != "nt", reason="Windows offline mirror helper")
@pytest.mark.parametrize("outcome", ["ready", "timeout", "mismatch", "dirty", "prefix_collision"])
def test_post_sync_mirror_is_bounded_and_never_blocks_aftercare(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str) -> None:
    helper = tmp_path / "scripts" / "maintain_aistock_git_mirror.ps1"
    helper.parent.mkdir()
    helper.write_text("# fixture", encoding="utf-8")
    monkeypatch.setenv("AISTOCK_GITHUB_RUNNER_PREBUILT_ROOT", str(tmp_path))
    sha = "a" * 9 + "b" * 31
    monkeypatch.setattr(workflow, "_git_snapshot", lambda root: {
        "branch": "main", "head": sha[:9], "origin_main": sha[:9], "dirty": outcome == "dirty"})
    calls: list[Any] = []
    def run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        calls.append((args, kwargs))
        if args[:2] == ["git", "rev-parse"]:
            return {"ok": True, "stdout": sha + "\n" + ("a" * 9 + "c" * 31 if outcome == "prefix_collision" else sha)}
        return {"ok": outcome != "timeout", "stdout": workflow.json.dumps({
            "status": "ready", "main_sha": sha if outcome == "ready" else "b" * 40,
            "network_accessed": False, "process_control_performed": False})}
    monkeypatch.setattr(workflow, "_run_command", run)
    receipt = workflow._refresh_ci_git_mirror_after_root_sync(tmp_path)
    assert receipt["status"] == ("ready" if outcome == "ready" else "warning")
    assert "blocking" not in receipt
    helper_calls = [call for call in calls if call[0][0] == "powershell.exe"]
    assert bool(helper_calls) == (outcome not in {"dirty", "prefix_collision"})
    if helper_calls:
        assert helper_calls[0][1]["timeout"] == 60 and "-Apply" in helper_calls[0][0]


@pytest.mark.parametrize("cleanup,gate", [(False, "close_sync_persisted"), (True, "complete"), (True, "fixed_source_pending_user_restart")])
def test_merge_wrapper_forwards_only_explicit_aftercare_and_does_not_recreate_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup: bool, gate: str) -> None:
    source, canonical = tmp_path / "source", tmp_path / "canonical"
    source.mkdir()
    canonical.mkdir()
    monkeypatch.setattr(workflow, "REPO_ROOT", source)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: canonical)
    monkeypatch.setattr(workflow, "_merge_pr_if_ready_for_bug", lambda *args: {"verified": {"checked": True}})
    states: list[Any] = []
    monkeypatch.setattr(workflow, "_write_state", lambda *args, **kwargs: states.append(kwargs))
    def finalizer(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["cleanup"] == cleanup and kwargs["merge_close_sync_pr"] == cleanup
        assert kwargs["source_pr_check"] == {"checked": True}
        if cleanup:
            source.rmdir()
        return {"workflow_gate": gate, "cleanup": {"workflow_gate": "cleanup_done"} if cleanup else None,
                "close_sync_pr_merge": {"workflow_gate": "merged"}, "source_merge_commit": "a" * 40}
    monkeypatch.setattr(workflow, "build_merge_finalizer_plan", finalizer)
    result = workflow.build_run_plan(bug_id="BUG-999", mode="merge", issue_json=None, changed_files=[], create_worktree=False,
        dry_run=False, validation_evidence=["python -m nox -s l0 -> passed"], task_slug=None, allow_missing_linkage=False,
        allow_closed=False, base="origin/main", head="HEAD", pr_url="https://github.com/licong01-cloud/AIstock/pull/1",
        merge=True, cleanup=cleanup, merge_close_sync_pr=cleanup, branch="bug/task", worktree=str(source))
    assert states[-1]["root"] == (canonical if cleanup else source)
    assert source.exists() != cleanup
    assert result["workflow_gate"] == ("merged_runtime_verification_pending" if "pending" in gate else "merged_close_synced")


def test_merge_aftercare_apply_flags_and_blocked_exit_are_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(workflow.sys, "argv", ["workflow.py", "run", "--bug-id", "BUG-999", "--mode", "merge", "--merge", "--cleanup", "--merge-close-sync-pr"])
    args = workflow.build_parser().parse_args()
    def plan(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["cleanup"] is True and kwargs["merge_close_sync_pr"] is True
        return {"workflow_gate": "merged_aftercare_blocked"}
    monkeypatch.setattr(workflow, "build_run_plan", plan)
    monkeypatch.setattr(workflow, "_emit_args", lambda *args: None)
    assert workflow.cmd_run(args) == 2


def test_finalizer_records_state_and_postmortem_outside_deleted_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, canonical = tmp_path / "source", tmp_path / "canonical"
    source.mkdir()
    canonical.mkdir()
    monkeypatch.setattr(workflow, "REPO_ROOT", source)
    monkeypatch.setattr(workflow, "_canonical_root", lambda: canonical)
    monkeypatch.setattr(workflow, "_cwd_is_inside", lambda _: False)
    monkeypatch.setattr(workflow, "_relocate_cwd_before_cleanup", lambda _: None)
    for name in ("_close_sync_is_complete", "_close_sync_pr_in_progress_marker", "_source_merge_receipt_from_close_sync"):
        monkeypatch.setattr(workflow, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(workflow, "_publish_changed_clients_after_merge", lambda **kwargs: {"workflow_gate": "ready"})
    monkeypatch.setattr(workflow, "build_close_sync_plan", lambda **kwargs: {"workflow_gate": "closed"})
    monkeypatch.setattr(workflow, "_persist_source_merge_receipt_for_close_sync", lambda plan, **kwargs: plan)
    monkeypatch.setattr(workflow, "_maybe_commit_and_pr_close_sync", lambda **kwargs: {"workflow_gate": "pr_opened"})
    monkeypatch.setattr(workflow, "_merge_close_sync_pr_if_ready", lambda **kwargs: {"workflow_gate": "merged"})
    def cleanup(**kwargs: Any) -> tuple[dict[str, Any], None]:
        source.rmdir()
        return {"workflow_gate": "cleanup_done", "sync_root": True}, None
    monkeypatch.setattr(workflow, "_build_cleanup_after_merge_plan_with_root_sync_deferral", cleanup)
    monkeypatch.setattr(workflow, "_build_close_sync_cleanup_after_merge_plan_with_root_sync_deferral", lambda **kwargs: ({"workflow_gate": "cleanup_done"}, None))
    states: list[Any] = []
    monkeypatch.setattr(workflow, "_write_state", lambda *args, **kwargs: states.append(kwargs))
    monkeypatch.setattr(workflow, "build_postmortem_plan", lambda **kwargs: {"worktree": kwargs["worktree"]})
    result = workflow.build_merge_finalizer_plan(bug_id="BUG-999", source_pr_url="https://github.com/licong01-cloud/AIstock/pull/1",
        source_branch="bug/task", source_worktree=str(source), validation_evidence=["python -m nox -s l0 -> passed"],
        cleanup=True, merge_close_sync_pr=True, apply=True, source_pr_check={"pr": {"mergeCommit": {"oid": "a" * 40}}})
    assert result["workflow_gate"] == "complete" and not source.exists()
    assert states[-1]["root"] == canonical and result["postmortem"]["worktree"] == str(canonical)


def test_merge_finalizer_stops_before_close_sync_when_client_publish_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        workflow,
        "_publish_changed_clients_after_merge",
        lambda **kwargs: {"workflow_gate": "blocked", "blocking": ["client publish failed"]},
    )
    monkeypatch.setattr(
        workflow,
        "build_close_sync_plan",
        lambda **kwargs: pytest.fail("close-sync started before client publish"),
    )

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        source_pr_url="https://github.example/pull/199",
        source_branch=None,
        source_worktree=None,
        validation_evidence=["pytest -> passed"],
        sync_root=True,
        apply=True,
        source_pr_check={
            "checked": True,
            "merged": True,
            "pr": {"mergeCommit": {"oid": "d" * 40}},
        },
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["blocking"] == ["client publish failed"]
