from uuid import uuid4

import pytest

from backend.services.factor_research.models import ResearchError
from backend.services.factor_research.runner import write_json
from backend.services.factor_research.service import ResearchService


class RecordedAttempt:
    """Only failure-path fixture; real transactions are tested on existing DEV."""
    def __init__(self, output, spec):
        self.start = {"payload_json": {"execution": {"output": str(output), "spec": spec}}}
        self.records = []

    def attempt(self, *args):
        return self.start

    def record(self, value):
        self.records.append(value)
        return {"ok": True, "applied": True, "result": value}


def attachment(tmp_path):
    task_id, attempt_id = str(uuid4()), str(uuid4())
    spec = {"task_id": task_id, "attempt_id": attempt_id,
            "candidates": [{"factor_name": "m_trial", "script": "reviewed.py"}]}
    result = {"status": "computed", "scope": "research_candidate", "task_id": task_id,
              "attempt_id": attempt_id, "request": spec, "candidates": []}
    value = {"task_id": task_id, "attempt_id": attempt_id, "record_id": str(uuid4()),
             "expected_revision": 2, "result_path": str(tmp_path / "result.json")}
    return spec, result, value


def test_attach_does_not_accept_empty_results_as_computed(tmp_path):
    spec, result, value = attachment(tmp_path)
    write_json(tmp_path / "result.json", result)
    repo = RecordedAttempt(tmp_path, spec)
    with pytest.raises(ResearchError):
        ResearchService(repo).attach(value)
    assert repo.records == []


def test_attach_does_not_read_another_attempt_file(tmp_path):
    spec, result, value = attachment(tmp_path)
    other = tmp_path / "other.json"
    write_json(other, result)
    value["result_path"] = str(other)
    with pytest.raises(ResearchError, match="does not belong"):
        ResearchService(RecordedAttempt(tmp_path, spec)).attach(value)


def test_result_file_cannot_be_overwritten(tmp_path):
    path = tmp_path / "result.json"
    write_json(path, {"status": "computed"})
    with pytest.raises(FileExistsError):
        write_json(path, {"status": "replacement"})
    assert 'replacement' not in path.read_text(encoding="utf-8")


def test_attach_receipt_write_failure_retains_computed_recovery_input(tmp_path, monkeypatch):
    from backend.services.factor_research import service as module
    from backend.tests.factor_research.test_contracts import spec

    value = spec(tmp_path)
    writes = []

    class Repo:
        def replay(self, request):
            return None

        def record(self, request):
            writes.append(request)
            return {"applied": True, "revision": 2}

    def computed(request, output):
        output.mkdir(parents=True)
        return {"status": "computed", "task_id": request["task_id"]}

    def write(path, payload):
        if path.name == "attach.json":
            raise OSError("injected receipt write failure")
        write_json(path, payload)

    monkeypatch.setattr(module, "execute", computed)
    monkeypatch.setattr(module, "write_json", write)
    with pytest.raises(ResearchError) as error:
        ResearchService(Repo()).run(value)
    assert error.value.code == "computed_not_recorded"
    assert error.value.context["attachment"]["result_path"] == error.value.context["result_path"]
    from pathlib import Path
    assert Path(error.value.context["result_path"]).is_file()
    assert len(writes) == 1
