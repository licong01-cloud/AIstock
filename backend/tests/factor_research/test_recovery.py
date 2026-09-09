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


def test_legacy_attempt_cannot_attach_undeclared_comparison(tmp_path):
    spec, result, value = attachment(tmp_path)
    folder = tmp_path / "m_trial"
    folder.mkdir()
    (folder / "values.h5").write_bytes(b"fixture")
    (folder / "factor.py").write_text("# fixture", encoding="utf-8")
    result["candidates"] = [{"factor_name": "m_trial", "scope": "research_candidate", "metrics": {},
                             "values": str(folder / "values.h5"),
                             "source_script": str(folder / "factor.py")}]
    result["research_comparison"] = {"schema_version": "factor_research_comparison_v1"}
    write_json(tmp_path / "result.json", result)
    with pytest.raises(ResearchError, match="undeclared comparison"):
        ResearchService(RecordedAttempt(tmp_path, spec)).attach(value)


def test_declared_comparison_attach_checks_identity_and_records_without_recompute(tmp_path):
    spec, result, value = attachment(tmp_path)
    comparison_spec = {
        "research_role": "predictive_increment", "horizon": "1d", "baseline": ["m_trial"],
        "candidate": "m_candidate", "controls": {"style": [], "neighbors": [], "categorical": []},
        "fit_windows": [{"start": "2026-01-01", "end": "2026-01-05",
                         "knowledge_cutoff": {"date": "2026-01-07", "phase": "post_close"}}],
        "evaluation_windows": [{"start": "2026-01-08", "end": "2026-01-09", "fit_window_index": 0}],
        "direction": {"source": "declared", "sign": 1, "locked_at": "2026-01-07"},
    }
    spec["candidates"].append({"factor_name": "m_candidate", "script": "reviewed_candidate.py"})
    spec["comparison"] = comparison_spec
    result["request"] = spec
    candidates = []
    for name in ("m_trial", "m_candidate"):
        folder = tmp_path / name
        folder.mkdir()
        (folder / "values.h5").write_bytes(b"fixture")
        (folder / "factor.py").write_text("# fixture", encoding="utf-8")
        candidates.append({"factor_name": name, "scope": "research_candidate", "metrics": {},
                           "values": str(folder / "values.h5"), "source_script": str(folder / "factor.py")})
    result["candidates"] = candidates
    result["research_comparison"] = {
        "schema_version": "factor_research_comparison_v1",
        "scope": "research_comparison_not_official_metrics_or_qe_result",
        **{key: comparison_spec[key] for key in (
            "research_role", "horizon", "baseline", "candidate", "controls", "fit_windows",
            "evaluation_windows", "direction",
        )},
        "windows": [{}],
    }
    write_json(tmp_path / "result.json", result)
    repository = RecordedAttempt(tmp_path, spec)
    assert ResearchService(repository).attach(value)["applied"] is True
    assert len(repository.records) == 1


def test_declared_comparison_attach_rejects_stale_window_identity(tmp_path):
    spec, result, value = attachment(tmp_path)
    comparison_spec = {
        "research_role": "predictive_increment", "horizon": "1d", "baseline": ["m_trial"],
        "candidate": "m_candidate", "controls": {"style": [], "neighbors": [], "categorical": []},
        "fit_windows": [], "evaluation_windows": [{"start": "2026-01-08", "end": "2026-01-09"}],
        "direction": {"source": "declared", "sign": 1, "locked_at": "2026-01-07"},
    }
    spec["candidates"].append({"factor_name": "m_candidate", "script": "reviewed_candidate.py"})
    spec["comparison"] = comparison_spec
    result["request"] = spec
    result["candidates"] = []
    for name in ("m_trial", "m_candidate"):
        folder = tmp_path / name
        folder.mkdir()
        (folder / "values.h5").write_bytes(b"fixture")
        (folder / "factor.py").write_text("# fixture", encoding="utf-8")
        result["candidates"].append({"factor_name": name, "scope": "research_candidate", "metrics": {},
                                     "values": str(folder / "values.h5"),
                                     "source_script": str(folder / "factor.py")})
    result["research_comparison"] = {
        "schema_version": "factor_research_comparison_v1",
        "scope": "research_comparison_not_official_metrics_or_qe_result",
        **{key: comparison_spec[key] for key in (
            "research_role", "horizon", "baseline", "candidate", "controls", "fit_windows", "direction",
        )},
        "evaluation_windows": [{"start": "2026-01-10", "end": "2026-01-11"}],
        "windows": [{}],
    }
    write_json(tmp_path / "result.json", result)
    with pytest.raises(ResearchError, match="wrong contract"):
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
