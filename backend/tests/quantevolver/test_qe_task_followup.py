"""Real-response-shaped contracts for the GET-only QE follow-up tool."""
from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from scripts import qe_task_followup as followup

TASK = "qe_existing"
IDENTITY = {"dataset_manifest_sha256": "a" * 64, "qlib_calendar_sha256": "b" * 64}


def config():
    return {
        "task_id": TASK, "status": "completed", "execution_policy": {"dataset_identity": IDENTITY},
        "loops": [{"loop_index": 1, "node_id": "cpu", "label": "B",
                   "dataset_summary": {"release_id": "release", "cutoff": "2026-10-30"},
                   "_qe_batch_execution_policy": {"dataset_identity": IDENTITY}}],
        "loop_statuses": [{"loop_index": 1, "node_id": "cpu", "status": "completed", "experiment_id": "exp1"}],
    }


def archives():
    return [{"run_id": "run1", "task_id": TASK, "loop_index": 1, "node_id": "cpu",
             "experiment_id": "exp1", "status": "completed", "research_valid": True}]


class API:
    def __init__(self, value=None, rows=None):
        self.value = value or config()
        self.rows = archives() if rows is None else rows
        self.calls = []
        self.failed = set()

    def get(self, path):
        self.calls.append(path)
        if any(key in path for key in self.failed):
            raise followup.ReadUnavailable("ConnectionError")
        if "custom-evo-config" in path:
            return deepcopy(self.value)
        if "qe-archive/runs" in path:
            return deepcopy(self.rows)
        if path == "/runtime-identity":
            return {"merge_commit": "c" * 40}
        raise AssertionError(path)


class Node:
    def __init__(self):
        self.recorder = {"recorder_id": "rec1"}
        self.files = {"mlruns/1/rec1/portfolio_analysis/" + f: ("raw-" + f).encode() for f in followup.ACCOUNT_FILES}
        self.downloads = []
        self.status = "completed"
        self.fail_status = False
        self.fail_file = None
        self.complete_catalog = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get_loop_status(self, task, loop):
        assert task == TASK and loop.startswith("Loop")
        if self.fail_status:
            raise RuntimeError("unavailable")
        return {"status": self.status, "receipt_status": self.status}

    async def get_workspace_file(self, task, loop, path):
        assert path == "qe_current_recorder.json"
        return deepcopy(self.recorder)

    async def list_workspace_files(self, task, loop):
        return {"catalog_completeness": "complete" if self.complete_catalog else "partial",
                "files": [{"relative_path": p, "sha256": hashlib.sha256(raw).hexdigest()} for p, raw in self.files.items()]}

    async def download_workspace_file_bytes(self, task, loop, path):
        self.downloads.append(path)
        if path == self.fail_file:
            raise RuntimeError("temporary read failure")
        return self.files[path]


def snapshot(api=None, node=None, previous=None):
    node = node or Node()
    return asyncio.run(followup.observe(TASK, api or API(), node_factory=lambda _: node, previous=previous))


def test_public_summary_does_not_need_hidden_manifest_fields():
    actual = snapshot()
    assert actual["execution_status"] == "completed"
    assert actual["observer_status"] == "OK"
    assert actual["loops"][0]["dataset"]["batch_identity"] == IDENTITY
    assert actual["warehouse"]["status"] == "RECORDED_VALID"
    assert actual["warehouse"]["loops"][0]["latest_attempt_check"] == "NOT_EXPOSED_BY_LIST_API"


def test_unknown_identity_remains_unknown_without_blocking_execution():
    value = config()
    value.pop("execution_policy")
    value["loops"][0].pop("_qe_batch_execution_policy")
    actual = snapshot(API(value))
    assert actual["loops"][0]["dataset"]["status"] == "UNKNOWN"
    assert actual["execution_status"] == "completed"
    assert actual["observer_status"] == "OK"


def test_nonempty_metadata_without_manifest_does_not_claim_a_recorded_binding():
    actual = followup.dataset_identity({"_qe_batch_execution_policy": {
        "dataset_identity": {"cutoff_trade_date": "2026-10-30"}}}, {})
    assert actual["status"] == "UNKNOWN"


def test_http_client_preserves_a_real_task_error_status_and_only_uses_get(monkeypatch):
    import requests

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {**config(), "status": "error"}

    class Session:
        def __init__(self):
            self.calls = []

        def get(self, url, **kwargs):
            self.calls.append(url)
            return Response()

        def close(self):
            pass

    session = Session()
    monkeypatch.setattr(requests, "Session", lambda: session)
    api = followup.MainAPI("http://unused/api/v1")
    result = api.get("/task")
    assert result["status"] == "error" and result["task_id"] == TASK
    assert session.calls == ["http://unused/api/v1/task"]


def test_identity_conflicts_are_reported_without_mutating_the_task():
    value = config()
    value["loops"][0]["_qe_batch_execution_policy"] = {"dataset_identity": {"dataset_manifest_sha256": "d" * 64}}
    api = API(value)
    actual = snapshot(api)
    assert actual["loops"][0]["dataset"]["status"] == "CONFLICT"
    assert actual["execution_status"] == "completed"
    assert api.value == value


def test_failed_observer_keeps_last_known_distinct_from_current_execution():
    api = API()
    previous = snapshot(api)
    api.failed.add("custom-evo-config")
    actual = snapshot(api, previous=previous)
    assert actual["observer_status"] == "UNAVAILABLE"
    assert actual["execution_status"] == "UNKNOWN"
    assert actual["last_known"]["execution_status"] == "completed"


def test_old_failed_attempt_does_not_replace_controller_completed_status():
    value = config()
    value["loops"].append({"loop_index": 2, "node_id": "cpu", "label": "OLD_B"})
    value["loop_statuses"].append({"loop_index": 2, "node_id": "cpu", "status": "failed"})
    actual = snapshot(API(value))
    assert actual["execution_status"] == "completed"
    assert [r["status"] for r in actual["loops"]] == ["completed", "failed"]
    assert actual["warehouse"]["status"] == "RECORDED_VALID"


def test_one_unavailable_node_does_not_hide_the_other_node():
    value = config()
    value["loops"].append({"loop_index": 2, "node_id": "gpu"})
    value["loop_statuses"].append({"loop_index": 2, "node_id": "gpu", "status": "running"})
    node1, node2 = Node(), Node()
    node1.fail_status = True
    actual = asyncio.run(followup.observe(TASK, API(value), node_factory=lambda n: node1 if n == "cpu" else node2))
    assert actual["loops"][0]["native"]["status"] == "UNAVAILABLE"
    assert actual["loops"][1]["native"]["status"] == "completed"


@pytest.mark.parametrize("service", ["runtime-identity", "qe-archive/runs"])
def test_auxiliary_endpoint_failure_never_turns_execution_into_failed(service):
    api = API()
    api.failed.add(service)
    actual = snapshot(api)
    assert actual["execution_status"] == "completed"
    assert actual["warnings"]


@pytest.mark.parametrize("change", ["missing_loops", "wrong_task", "duplicate_status"])
def test_malformed_api_data_is_observer_error_not_experiment_failure(change):
    value = config()
    if change == "missing_loops":
        value.pop("loops")
    elif change == "wrong_task":
        value["task_id"] = "different"
    else:
        value["loop_statuses"].append(value["loop_statuses"][0])
    actual = snapshot(API(value))
    assert actual["observer_status"] == "UNAVAILABLE"
    assert actual["execution_status"] == "UNKNOWN"


@pytest.mark.parametrize("change", ["wrong_task", "wrong_experiment", "wrong_node", "duplicate", "invalid"])
def test_archive_never_uses_wrong_source_or_guesses_latest(change):
    rows = archives()
    if change == "duplicate":
        rows.append({**rows[0], "run_id": "another"})
    elif change == "invalid":
        rows[0]["research_valid"] = False
    else:
        field = {"wrong_task": "task_id", "wrong_experiment": "experiment_id", "wrong_node": "node_id"}[change]
        rows[0][field] = "wrong"
    actual = snapshot(API(rows=rows))
    assert actual["execution_status"] == "completed"
    assert actual["warehouse"]["status"] == "INCOMPLETE"


def test_archive_does_not_claim_exact_source_when_experiment_identity_is_missing():
    value = config()
    value["loop_statuses"][0].pop("experiment_id")
    actual = snapshot(API(value))
    assert actual["warehouse"]["status"] == "INCOMPLETE"
    assert actual["warehouse"]["loops"][0]["status"] == "SOURCE_IDENTITY_UNAVAILABLE"


def test_controller_node_conflict_does_not_collect_a_still_changing_account(tmp_path):
    node = Node()
    node.status = "running"
    actual = snapshot(node=node)
    asyncio.run(followup.collect_accounts(actual, config(), followup.ArtifactStore(tmp_path), node_factory=lambda _: node))
    assert actual["execution_status"] == "completed"
    assert actual["loops"][0]["native"]["status"] == "running"
    assert actual["loops"][0]["collection"]["reason_code"] == "SOURCE_COMPLETION_UNCONFIRMED"
    assert node.downloads == []


def test_restarting_collection_reuses_verified_files_without_downloading(tmp_path):
    node = Node()
    store = followup.ArtifactStore(tmp_path)
    first = snapshot(node=node)
    asyncio.run(followup.collect_accounts(first, config(), store, node_factory=lambda _: node))
    assert first["collection"]["status"] == "COLLECTED"
    assert len(node.downloads) == 3
    second = snapshot(node=node)
    asyncio.run(followup.collect_accounts(second, config(), store, node_factory=lambda _: node))
    assert second["collection"]["status"] == "COLLECTED"
    assert len(node.downloads) == 3
    assert all(r["reused"] for r in second["loops"][0]["collection"]["files"])


def test_partial_collection_recovers_only_unread_files(tmp_path):
    node = Node()
    node.fail_file = list(node.files)[1]
    store = followup.ArtifactStore(tmp_path)
    first = snapshot(node=node)
    asyncio.run(followup.collect_accounts(first, config(), store, node_factory=lambda _: node))
    assert first["execution_status"] == "completed"
    assert first["collection"]["status"] == "INCOMPLETE"
    node.fail_file = None
    second = snapshot(node=node)
    asyncio.run(followup.collect_accounts(second, config(), store, node_factory=lambda _: node))
    assert second["collection"]["status"] == "COLLECTED"
    assert node.downloads.count(list(node.files)[0]) == 1


def test_corrupt_cached_account_is_refetched_without_overwriting_old_bytes(tmp_path):
    node = Node()
    store = followup.ArtifactStore(tmp_path)
    first = snapshot(node=node)
    asyncio.run(followup.collect_accounts(first, config(), store, node_factory=lambda _: node))
    old = Path(first["loops"][0]["collection"]["files"][0]["local_path"])
    old.write_bytes(b"damaged")
    second = snapshot(node=node)
    asyncio.run(followup.collect_accounts(second, config(), store, node_factory=lambda _: node))
    assert second["collection"]["status"] == "COLLECTED"
    assert len(node.downloads) == 4
    assert old.read_bytes() == b"damaged"


def test_changed_recorder_cannot_reuse_old_account_files(tmp_path):
    node = Node()
    store = followup.ArtifactStore(tmp_path)
    asyncio.run(followup.collect_accounts(snapshot(node=node), config(), store, node_factory=lambda _: node))
    node.recorder = {"recorder_id": "rec2"}
    node.files = {p.replace("/rec1/", "/rec2/"): raw for p, raw in node.files.items()}
    actual = snapshot(node=node)
    asyncio.run(followup.collect_accounts(actual, config(), store, node_factory=lambda _: node))
    assert len(node.downloads) == 6
    assert actual["loops"][0]["collection"]["recorder"]["recorder_id"] == "rec2"


def test_changed_source_sha_does_not_repeatedly_choose_an_older_valid_cache(tmp_path):
    node = Node()
    store = followup.ArtifactStore(tmp_path)
    asyncio.run(followup.collect_accounts(snapshot(node=node), config(), store, node_factory=lambda _: node))
    path = list(node.files)[0]
    node.files[path] = b"new source bytes"
    for _ in range(2):
        actual = snapshot(node=node)
        asyncio.run(followup.collect_accounts(actual, config(), store, node_factory=lambda _: node))
        assert actual["collection"]["status"] == "COLLECTED"
    assert node.downloads.count(path) == 2


def test_incomplete_catalog_is_not_filled_with_guessed_file_paths(tmp_path):
    node = Node()
    node.complete_catalog = False
    actual = snapshot(node=node)
    asyncio.run(followup.collect_accounts(actual, config(), followup.ArtifactStore(tmp_path), node_factory=lambda _: node))
    assert not node.downloads
    assert actual["loops"][0]["collection"]["reason_code"] == "ACCOUNT_CATALOG_INCOMPLETE"
    assert actual["execution_status"] == "completed"


def test_absolute_account_reuses_existing_contract_validation_and_actions_hash(tmp_path, monkeypatch):
    from backend.services.quantevolver import absolute_target_consumer

    seen = []
    node = Node()
    actions = b'{"trade_date":"2026-10-29"}\n'
    node.files["qe_absolute_target_actions.jsonl"] = actions
    node.files["qe_absolute_target_result.json"] = json.dumps({"actions_sha256": followup.sha(actions)}).encode()
    value = config()
    contract = {"schema_version": "qe_absolute_target_replay_v2"}
    value["loops"][0]["absolute_target_replay"] = contract
    monkeypatch.setattr(absolute_target_consumer, "validate_absolute_result", lambda result, spec: seen.append(spec))
    actual = snapshot(API(value), node)
    asyncio.run(followup.collect_accounts(actual, value, followup.ArtifactStore(tmp_path), node_factory=lambda _: node))
    assert seen == [contract]
    assert actual["collection"]["status"] == "COLLECTED"


def test_optional_analysis_failure_cannot_change_account_status(tmp_path):
    actual = snapshot()
    followup.attach_analysis(actual, tmp_path / "missing.json")
    assert actual["execution_status"] == "completed"
    assert actual["analysis"]["status"] == "UNAVAILABLE"
    assert actual["analysis"]["economic_acceptance"] == "NOT_EVALUATED"


def test_available_analysis_receipt_is_not_economic_acceptance(tmp_path):
    path = tmp_path / "analysis.json"
    path.write_text(json.dumps({"task_id": TASK, "status": "PASSED"}), encoding="utf-8")
    actual = snapshot()
    followup.attach_analysis(actual, path)
    assert actual["analysis"]["status"] == "RECEIPT_AVAILABLE"
    assert actual["analysis"]["economic_acceptance"] == "NOT_EVALUATED"


def test_local_report_failure_is_only_a_warning(tmp_path, monkeypatch):
    actual = snapshot()
    monkeypatch.setattr(Path, "mkdir", lambda *a, **k: (_ for _ in ()).throw(PermissionError()))
    followup.write_snapshot(tmp_path, actual)
    assert actual["execution_status"] == "completed"
    assert actual["warnings"][-1]["code"] == "LOCAL_REPORT_WRITE_UNAVAILABLE"


def test_watch_retries_reads_after_disconnect_and_stops_on_real_completion(tmp_path):
    api = API()
    api.failed.add("custom-evo-config")
    snapshots = []
    sleeps = []

    async def recover(seconds):
        sleeps.append(seconds)
        api.failed.clear()

    args = argparse.Namespace(task_id=TASK, output_dir=tmp_path, collect_accounts=False,
                              analysis_receipt=None, watch=True, poll_seconds=45)
    actual = asyncio.run(followup.follow(args, api, node_factory=lambda _: Node(), emit=snapshots.append, sleep=recover))
    assert len(snapshots) == 2 and sleeps == [45]
    assert actual["execution_status"] == "completed"
    assert all(p.startswith("/") for p in api.calls)


def test_no_submission_process_control_or_database_mutation_capability_is_added():
    import ast

    tree = ast.parse(Path(followup.__file__).read_text(encoding="utf-8"))
    forbidden = {"post", "patch", "delete", "execute", "Popen", "run", "system", "kill", "fit", "predict", "backtest"}
    calls = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    # asyncio.run executes the local reader; there is no subprocess.run.
    calls.discard("run")
    assert not calls & forbidden
    assert not any(isinstance(n, (ast.Import, ast.ImportFrom)) and any(
        name.name in {"subprocess", "psycopg2"} for name in n.names) for n in ast.walk(tree))
