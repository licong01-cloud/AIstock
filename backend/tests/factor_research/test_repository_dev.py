"""Collected in source CI, executed only by explicitly authorized DEV validation."""
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest

from backend.services.factor_research.models import ResearchError

pytestmark = pytest.mark.skipif(
    os.environ.get("AISTOCK_DEV_DB_E2E") != "1",
    reason="Existing DEV database validation requires AISTOCK_DEV_DB_E2E=1",
)


@pytest.fixture(scope="module")
def repo():
    from backend.services.factor_research.repository import ResearchRepository
    from scripts.factor_research import configure

    env_file = os.environ.get("FACTOR_RESEARCH_DEV_ENV_FILE")
    if not env_file:
        pytest.fail("Authorized DEV validation requires FACTOR_RESEARCH_DEV_ENV_FILE")
    configure(Path(env_file), "dev")
    return ResearchRepository()


def create(repo):
    value = {"task_id": str(uuid4()), "record_id": str(uuid4()), "summary": "P1 DEV事务验证",
             "task": {"title": "P1 DEV验证", "objective": "幂等与恢复", "task_type": "diagnosis",
                      "factor_names": ["m_p1_dev"], "context_json": {"test": True, "method_version": "1.0"}}}
    return value, repo.create(value)


def update(task_id, revision=1, **kwargs):
    return {"task_id": task_id, "record_id": str(uuid4()), "summary": "进度验证", "expected_revision": revision,
            "record_type": "progress", "task_update": {"next_action": "读回"}, **kwargs}


def test_create_and_response_loss_are_idempotent(repo):
    req, result = create(repo)
    replay = repo.create(req)
    assert result["applied"] and replay["replayed"] and replay["revision"] == 1
    assert len(repo.show(req["task_id"])["records"]) == 1
    req["task"]["title"] = "不同请求"
    with pytest.raises(ResearchError, match="different request"):
        repo.create(req)


@pytest.mark.parametrize("same_request", [True, False])
def test_concurrent_writes_are_idempotent_or_conflict(repo, same_request):
    req, _ = create(repo)
    first = update(req["task_id"])
    def write(record):
        try:
            return repo.record(record)["applied"]
        except ResearchError as exc:
            return exc.code
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(write, [first, first if same_request else update(req["task_id"])]))
    assert results.count(True) == 1 and results.count(False if same_request else "revision_conflict") == 1
    assert repo.show(req["task_id"])["task"]["revision"] == 2


def test_record_failure_rolls_back_task_projection(repo, monkeypatch):
    req, _ = create(repo)
    def fail(*args):
        raise RuntimeError("injected after task update")
    monkeypatch.setattr(repo, "_insert_record", fail)
    with pytest.raises(RuntimeError, match="injected"):
        repo.record(update(req["task_id"]))
    result = repo.show(req["task_id"])
    assert result["task"]["revision"] == 1 and len(result["records"]) == 1


def test_distinct_request_cannot_restart_attempt(repo):
    req, _ = create(repo)
    attempt_id = str(uuid4())
    first = update(req["task_id"], record_type="attempt", attempt_id=attempt_id)
    result = repo.record(first)
    second = {**first, "record_id": str(uuid4()), "expected_revision": result["revision"]}
    with pytest.raises(ResearchError) as error:
        repo.record(second)
    assert error.value.code == "attempt_exists"
    assert repo.show(req["task_id"])["task"]["revision"] == 2


def test_discovery_correction_and_same_task_relation(repo):
    req, created = create(repo)
    correction = update(req["task_id"], record_type="correction", related_record_id=created["record_id"])
    repo.record(correction)
    other, _ = create(repo)
    with pytest.raises(ResearchError) as error:
        repo.record(update(other["task_id"], record_type="correction", related_record_id=created["record_id"]))
    assert error.value.code == "invalid_relation"
    discovered = repo.list(factor="m_p1_dev", query="P1 DEV", limit=100)
    assert req["task_id"] in {str(row["task_id"]) for row in discovered["tasks"]}
    page = repo.show(req["task_id"], limit=1)
    assert page["before_revision"] == 2
    assert repo.show(req["task_id"], before_revision=2)["records"][0]["record_type"] == "created"


def test_memory_sql_corrections_snapshot_and_no_writes(repo, monkeypatch):
    req, _ = create(repo)
    task, token, ids = req["task_id"], "memory_%_" + str(uuid4()), []
    query = dict(schema_version="factor_research_memory_query_v1", query=dict(problem="DEV contract", terms=[token]),
                 filters={"horizons": ["20d"]})
    try:
        first = update(task, summary=token, payload={"experience_note": {"conditions": {"horizons": ["20d"]}}},
                       task_update={"context_json": {"horizons": ["1d"]}})
        # The original request explicitly disagrees; old rows must not inherit current task state either.
        repo.record(first)
        ids.append(first["record_id"])
        for rev in (2, 3):
            correction = update(task, rev, record_type="correction", related_record_id=ids[0], summary="更正而非关键词")
            repo.record(correction)
            ids.append(correction["record_id"])
        before = repo.show(task)
        result = repo.memory_search(query, target="dev")
        assert result["matched_count"] == 1 and result["unknown_filter_count"] == 1
        assert result["relations_complete"] is True and len(result["related_entries"]) == 2
        assert "condition_conflict:horizons" in result["entries"][0]["missing_information"]
        assert {e["source_ref"]["locator"]["record_id"] for e in result["related_entries"]} == set(ids[1:])
        assert repo.show(task) == before
        with repo.cursor() as cur:
            rows, reasons = repo._memory_component(cur, task, ids[0], max_records=1)
            assert len(rows) == 1 and "relations_truncated_use_show" in reasons
        original = repo._memory_component
        def concurrent_correction(cur, task_id, record_id):
            cur.execute("SHOW transaction_isolation")
            assert cur.fetchone()["transaction_isolation"] == "repeatable read"
            cur.execute("SHOW transaction_read_only")
            assert cur.fetchone()["transaction_read_only"] == "on"
            new = update(task, 4, record_type="correction", related_record_id=ids[0])
            repo.record(new)
            ids.append(new["record_id"])
            return original(cur, task_id, record_id)
        with monkeypatch.context() as patch:
            patch.setattr(repo, "_memory_component", concurrent_correction)
            assert len(repo.memory_search(query, target="dev")["related_entries"]) == 2
        assert len(repo.memory_search(query, target="dev")["related_entries"]) == 3
        def fail(*args, **kwargs):
            raise RuntimeError("injected read failure")
        with monkeypatch.context() as patch:
            patch.setattr(repo, "_memory_component", fail)
            with pytest.raises(RuntimeError, match="injected"):
                repo.memory_search(query, target="dev")
        with repo.cursor() as cur:
            cur.execute("SHOW transaction_isolation")
            assert cur.fetchone()["transaction_isolation"] == "read committed"
        assert repo.show(task)["task"]["revision"] == 5
        with repo.cursor(write=True) as cur:
            cur.execute("UPDATE public.factor_research_records SET related_record_id=%s WHERE record_id=%s", (ids[1], ids[0]))
        assert "relation_cycle" in repo.memory_search(query, target="dev")["entries"][0]["relation_reasons"]
        other, _ = create(repo)
        try:
            with repo.cursor(write=True) as cur:
                cur.execute("UPDATE public.factor_research_records SET related_record_id=%s WHERE record_id=%s", (other["record_id"], ids[0]))
            isolated = repo.memory_search(query, target="dev")
            assert "cross_task_relation" in isolated["entries"][0]["relation_reasons"]
            assert all(e["source_ref"]["locator"]["task_id"] == task for e in isolated["related_entries"])
        finally:
            with repo.cursor(write=True) as cur:
                cur.execute("UPDATE public.factor_research_records SET related_record_id=NULL WHERE record_id=%s", (ids[0],))
                cur.execute("DELETE FROM public.factor_research_records WHERE task_id=%s", (other["task_id"],))
                cur.execute("DELETE FROM public.factor_research_tasks WHERE task_id=%s", (other["task_id"],))
    finally:
        with repo.cursor(write=True) as cur:
            cur.execute("DELETE FROM public.factor_research_records WHERE task_id=%s", (task,))
            cur.execute("DELETE FROM public.factor_research_tasks WHERE task_id=%s", (task,))


def test_computed_result_recovers_on_real_dev_without_reexecution(repo, monkeypatch, tmp_path):
    import json
    import pandas as pd
    from backend.services.factor_research import service as module
    from backend.services.factor_research.runner import CANONICAL_UNIVERSE

    req, _ = create(repo)
    data = tmp_path / "data"
    data.mkdir()
    code = tmp_path / "reviewed.py"
    code.write_text("# failure-recovery fixture\n", encoding="utf-8")
    spec = {"task_id": req["task_id"], "record_id": str(uuid4()), "attempt_id": str(uuid4()),
            "expected_revision": 1, "method_version": "1.0", "universe_key": CANONICAL_UNIVERSE,
            "data_dir": str(data), "qlib_bin_path": str(data), "artifact_root": str(tmp_path / "out"),
            "instruments": ["000001.SZ"], "read_start": "2025-01-01", "signal_start": "2025-01-01",
            "signal_end": "2025-01-03", "read_end": "2025-01-03", "cutoff": "2025-01-03",
            "candidates": [{"factor_name": "m_fixture", "script": str(code)}]}
    executions = []
    def computed(value, output):
        executions.append(value["attempt_id"])
        folder = output / "m_fixture"
        folder.mkdir(parents=True)
        (folder / "factor.py").write_text(code.read_text(), encoding="utf-8")
        index = pd.MultiIndex.from_product([pd.date_range("2025-01-01", periods=3), ["000001.SZ"]],
                                          names=["datetime", "instrument"])
        pd.DataFrame({"m_fixture": [1., 2., 3.]}, index=index).to_hdf(folder / "values.h5", key="data")
        return {"task_id": value["task_id"], "attempt_id": value["attempt_id"], "status": "computed",
                "scope": "research_candidate", "request": value,
                "candidates": [{"factor_name": "m_fixture", "scope": "research_candidate",
                                "metrics": {"reports": [{"reason": "failure_injection_fixture"}]},
                                "values": str(folder / "values.h5"), "source_script": str(folder / "factor.py")}]}
    monkeypatch.setattr(module, "execute", computed)
    original = repo.record
    def fail_result(value):
        if value["record_type"] == "result":
            raise ConnectionError("injected DEV write outage after computation")
        return original(value)
    monkeypatch.setattr(repo, "record", fail_result)
    service = module.ResearchService(repo)
    with pytest.raises(ResearchError) as error:
        service.run(spec)
    assert error.value.code == "computed_not_recorded"
    monkeypatch.setattr(repo, "record", original)
    attachment = json.loads(Path(error.value.context["attach_path"]).read_text(encoding="utf-8"))
    assert service.attach(attachment)["applied"]
    assert service.attach(attachment)["replayed"]
    assert len(executions) == 1
    assert repo.show(req["task_id"])["task"]["revision"] == 3
