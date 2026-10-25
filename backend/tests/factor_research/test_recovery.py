"""Compact recovery contracts: exact identity, immutable results and attach-only resume."""

import json
from pathlib import Path
from uuid import uuid4

import pytest

from backend.services.factor_research.models import ResearchError
from backend.services.factor_research.runner import write_json
from backend.services.factor_research.service import ResearchService


class Repository:
    def __init__(self, output, spec):
        self.start = {"payload_json": {"execution": {"output": str(output), "spec": spec}}}
        self.records = []

    def attempt(self, *_args):
        return self.start

    def replay(self, _request):
        return None

    def record(self, value):
        self.records.append(value)
        return {"applied": True, "revision": 2, "result": value}


def recovered(tmp_path, reuse=False):
    task_id, attempt_id = str(uuid4()), str(uuid4())
    spec = {
        "task_id": task_id,
        "attempt_id": attempt_id,
        "candidates": [{"factor_name": "m_trial", "script": "reviewed.py"}],
    }
    folder = tmp_path / "m_trial"
    folder.mkdir()
    (folder / "values.h5").write_bytes(b"fixture")
    (folder / "factor.py").write_text("# reviewed", encoding="utf-8")
    result = {
        "status": "computed",
        "scope": "research_candidate",
        "task_id": task_id,
        "attempt_id": attempt_id,
        "request": spec,
        "candidates": [
            {
                "factor_name": "m_trial",
                "scope": "research_candidate",
                "metrics": {},
                "values": str(folder / "values.h5"),
                "source_script": str(folder / "factor.py"),
            }
        ],
    }
    value = {
        "task_id": task_id,
        "attempt_id": attempt_id,
        "record_id": str(uuid4()),
        "expected_revision": 2,
        "result_path": str(tmp_path / "result.json"),
    }
    if reuse:
        spec["candidates"][0].update(values_artifact=str(folder / "values.h5"), reuse_basis="reviewed")
        result["candidates"][0].update(values_origin="reused_reviewed_artifact", reuse_basis="reviewed")
    return spec, result, value


@pytest.mark.parametrize(
    "mode,match",
    [
        ("empty", "every candidate"),
        ("foreign", "does not belong"),
        ("comparison", "undeclared comparison"),
    ],
)
def test_attach_fails_closed_on_incomplete_or_foreign_identity(tmp_path, mode, match):
    spec, result, value = recovered(tmp_path)
    if mode == "empty":
        result["candidates"] = []
    elif mode == "foreign":
        value["result_path"] = str(tmp_path / "other.json")
    else:
        result["research_comparison"] = {"schema_version": "factor_research_comparison_v1"}
    write_json(Path(value["result_path"]), result)
    repo = Repository(tmp_path, spec)
    with pytest.raises(ResearchError, match=match):
        ResearchService(repo).attach(value)
    assert repo.records == []


@pytest.mark.parametrize("reuse", [False, True])
def test_exact_result_attaches_once(tmp_path, reuse):
    spec, result, value = recovered(tmp_path, reuse)
    write_json(tmp_path / "result.json", result)
    repo = Repository(tmp_path, spec)
    assert ResearchService(repo).attach(value)["applied"] and len(repo.records) == 1
    if reuse:
        result["candidates"][0]["reuse_basis"] = "different"
        (tmp_path / "result.json").write_text(json.dumps(result), encoding="utf-8")
        with pytest.raises(ResearchError, match="Reuse"):
            ResearchService(repo).attach(value)


def test_computed_result_survives_record_failure_without_reexecution(tmp_path, monkeypatch):
    from backend.services.factor_research import service as module
    from backend.tests.factor_research.test_contracts import spec as run_spec

    value, executions = run_spec(tmp_path), []
    output = Path(value["artifact_root"]) / value["task_id"] / value["attempt_id"]
    repository = Repository(output, value)
    original_record = repository.record

    def fail_result_record(request):
        if request["record_type"] == "result":
            raise ConnectionError("injected write outage")
        return original_record(request)

    repository.record = fail_result_record

    def computed(request, output):
        executions.append(request["attempt_id"])
        output.mkdir(parents=True)
        return {
            "status": "computed",
            "scope": "research_candidate",
            "task_id": request["task_id"],
            "attempt_id": request["attempt_id"],
            "request": request,
            "candidates": [],
        }

    monkeypatch.setattr(module, "execute", computed)
    with pytest.raises(ResearchError) as error:
        ResearchService(repository).run(value)
    assert error.value.code == "computed_not_recorded"
    assert Path(error.value.context["result_path"]).is_file() and len(executions) == 1
