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


def test_concurrent_same_request_one_write(repo):
    req, _ = create(repo)
    record = update(req["task_id"])
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(repo.record, [record, record]))
    assert sum(result["applied"] for result in results) == 1
    assert sum(result["replayed"] for result in results) == 1
    assert repo.show(req["task_id"])["task"]["revision"] == 2


def test_concurrent_different_updates_do_not_overwrite(repo):
    req, _ = create(repo)
    def write(record):
        try:
            return repo.record(record)["applied"]
        except ResearchError as exc:
            return exc.code
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(write, [update(req["task_id"]), update(req["task_id"])]))
    assert results.count(True) == 1 and results.count("revision_conflict") == 1


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


def test_read_cursor_is_enforced_readonly(repo):
    with repo.cursor() as cur:
        cur.execute("SHOW transaction_read_only")
        assert cur.fetchone()["transaction_read_only"] == "on"


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
