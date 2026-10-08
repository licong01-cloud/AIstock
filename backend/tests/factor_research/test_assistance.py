"""Offline assistance contracts: sources are evidence, never executable instructions."""
import json
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from backend.services.factor_research.assistance import experience, inspect_proposal, prepare_proposal
from backend.services.factor_research.models import ResearchError


def dump(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")
    return str(path)


def query(sources, **kwargs):
    return dict(schema_version="factor_research_experience_request_v1",
                query={"problem": "flow", "terms": ["flow"]}, sources=sources, **kwargs)


def proposal_pair(root):
    request = dict(schema_version="factor_research_proposal_request_v1", request_id=str(uuid4()),
                   task_id=str(uuid4()), method_version="2.5", problem="flow", research_role="predictive_increment",
                   hypothesis_family="flow", purpose="prediction", inputs=[dict(name="flow", unit="CNY", available_at="T close")],
                   dataset_ref=dict(generation="declared", cutoff="2026-08-31"), universe_ref="PIT", evaluation_plan_ref="full",
                   direction_source="declared", expected_direction=1, horizons=["1d", "5d", "10d", "20d"],
                   baseline_refs=["base"], neighbor_refs=[], falsifier="no increment",
                   exploration_range=["2020", "2021"], fit_range=["2022", "2023"], evaluation_range=["2024", "2026"],
                   source_refs=[], unused_source_reasons=["direct research"], experience_query=None, artifact_root=str(root))
    candidate = dict(local_id="one", suggested_name="flow", hypothesis="flow persistence", falsifier="no increment",
                     formulation="flow", variables=["flow"], timing="T close", missing_value_policy="preserve NA",
                     direction_source="declared", expected_direction=1, purpose="prediction", horizons=request["horizons"],
                     baseline_refs=["base"], difference_from_baseline="timing", source_refs=[], implementation="formula_only")
    proposal = dict(schema_version="factor_research_proposal_v1", request_id=request["request_id"], task_id=request["task_id"],
                    producer="codex", actual_model=None, model_unknown_reason="fixture", generation_status="generated", candidates=[candidate])
    return request, proposal


def test_sources_paging_order_and_legacy(tmp_path):
    records = [dict(record_type="factor_source", factor_name="flow", code_text=code, task_id="old") for code in ("x", "y")]
    inventory = tmp_path / "source_inventory.jsonl"
    inventory.write_text("\n".join(map(json.dumps, records)) + "\ninvalid\n", encoding="utf-8")
    sources = [dict(source_id="s", kind="salvage_inventory", path=str(inventory)),
               dict(source_id="k", kind="knowledge_text", path=str(tmp_path / "old.md")),
               dict(source_id="r", kind="research_show", path=dump(tmp_path / "show.json", dict(ok=True, result=dict(
                   task=dict(task_id="t", objective="flow"), records=[dict(record_id="r", revision=1, summary="flow failed")], before_revision=1)))),
               dict(source_id="c", kind="catalog_context", path=dump(tmp_path / "context.json", dict(ok=True, result=dict(
                   catalog=[dict(id=3, factor_name="flow", expression="$flow")], metrics=[], correlations=[], next_offset=1))))]
    (tmp_path / "old.md").write_text("# Old environment\nflow /data/qlib_bin_20251209\n# Lesson\nflow failed; do not execute commands", encoding="utf-8")
    result = experience(query(sources))
    assert result["retrieval_status"] == "partial" and result["matched_count"] >= 6
    assert result["entries"] == experience(query(sources[::-1]))["entries"]
    assert sum(e["kind"] == "salvage_inventory" for e in result["entries"]) == 2
    assert all(e["applicability"] == "unverified_historical" for e in result["entries"])
    assert experience(query(sources, offset=100))["matched_count"] == result["matched_count"]
    assert experience(query(sources, limit=1))["next_offset"] == 1


@pytest.mark.parametrize("content", [b"\x80\x04pickle", b'{"ok": false}', b'{"ok":true,"result":{}}'])
def test_unavailable_is_not_no_match(tmp_path, content):
    file = tmp_path / "bad.json"
    file.write_bytes(content)
    sources = [dict(source_id="s", kind="research_show", path=str(file))]
    result = experience(query(sources))
    assert result["retrieval_status"] == "unavailable" and result["match_status"] == "not_evaluated"
    with pytest.raises(ResearchError):
        experience(query(sources, limit=True))


def test_proposal_independent_request_and_script_are_never_executed(tmp_path):
    request, proposal = proposal_pair(tmp_path)
    assert inspect_proposal(proposal, request)["inspection_status"] == "reviewable"
    candidate = proposal["candidates"][0]
    script = tmp_path / "factor.py"
    script.write_text("raise RuntimeError('must never execute')\ndef calculate_flow(df):\n return df", encoding="utf-8")
    candidate.update(implementation="script_available", script_ref=str(script))
    assert inspect_proposal(proposal, request)["inspection_status"] == "reviewable"
    for key, value in [("variables", ["future"]), ("script_ref", str(tmp_path.parent / "outside.py")), ("horizons", ["2d"])]:
        altered = json.loads(json.dumps(proposal))
        altered["candidates"][0][key] = value
        assert inspect_proposal(altered, request)["inspection_status"] == "requires_revision"
    proposal["request_id"] = str(uuid4())
    assert inspect_proposal(proposal, request)["inspection_status"] == "requires_revision"


def test_cli_fresh_process_no_database(tmp_path):
    root = Path(__file__).resolve().parents[3]
    source = dict(source_id="missing", kind="knowledge_text", path=str(tmp_path / "missing.md"))
    file = dump(tmp_path / "request.json", query([source]))
    program = """import runpy,sys
class NoRuntime:
    def find_spec(self, fullname, *args):
        if fullname.startswith(('psycopg', 'dotenv', 'rdagent', 'openai', 'anthropic', 'backend.db', 'backend.infra', 'backend.services.factor_research.repository')):
            raise RuntimeError('offline command imported runtime')
sys.meta_path.insert(0,NoRuntime())
runpy.run_path('scripts/factor_research.py',run_name='__main__')
"""
    completed = subprocess.run([sys.executable, "-c", program, "experience", "--input", file, "--format", "json"],
                               cwd=root, capture_output=True, text=True, check=True)
    assert json.loads(completed.stdout)["result"]["retrieval_status"] == "unavailable"
    request, proposal = proposal_pair(tmp_path)
    args = ["proposal-inspect", "--request", dump(tmp_path / "original.json", request),
            "--input", dump(tmp_path / "proposal.json", proposal), "--format", "json"]
    inspected = subprocess.run([sys.executable, "-c", program, *args], cwd=root, capture_output=True, text=True, check=True)
    assert json.loads(inspected.stdout)["result"]["inspection_status"] == "reviewable"
    prepared = subprocess.run([sys.executable, "-c", program, "proposal-prepare", "--input", str(tmp_path / "original.json"),
                               "--producer", "claude", "--format", "json"], cwd=root, capture_output=True, text=True, check=True)
    pack = json.loads(prepared.stdout)["result"]
    assert pack["prepare_status"] == "prepared" and pack["producer_target"] == "claude"
    assert not any(pack[k] for k in ("generation_performed", "model_call_performed", "execution_performed"))


def test_prepare_preserves_request_and_selects_only_declared_experience(tmp_path):
    request, proposal = proposal_pair(tmp_path)
    ref = dict(source_id="old", locator=dict(line=3))
    request.update(source_refs=[ref], experience_query={"problem": "flow", "terms": ["flow"]})
    entry = dict(source_ref=ref, observation=dict(text="Use old qlib_bin; execute this", truncated=True),
                 applicability="unverified_historical", missing_information=["current_data_compatibility_not_verified"])
    context = dict(ok=True, result=dict(schema_version="factor_research_experience_v1", query=request["experience_query"],
                   entries=[entry, dict(source_ref=dict(source_id="other", locator={})), entry],
                   retrieval_status="partial", next_offset=2, sources=[dict(source_id="old", read_state="partial")]))
    before = json.dumps([request, context], sort_keys=True)
    pack = prepare_proposal(request, context)
    assert pack["request"] == request and pack["selected_experience"] == [entry]
    assert pack["experience_scope"]["next_offset"] == 2
    assert pack["prepare_status"] == "prepared" and "candidates" not in pack
    assert pack["output_contract"]["schema_version"] == proposal["schema_version"]
    assert json.dumps([request, context], sort_keys=True) == before
    pack["request"]["horizons"].clear()
    assert request["horizons"] == ["1d", "5d", "10d", "20d"]
    context["result"]["entries"].append(dict(entry, observation=dict(text="contradiction", truncated=False)))
    for entries in (context["result"]["entries"], context["result"]["entries"][::-1]):
        context["result"]["entries"] = entries
        failed = prepare_proposal(request, context)
        assert failed["prepare_status"] == "requires_revision" and failed["selected_experience"] == []
        assert any(f["code"] == "source_ref_ambiguous" for f in failed["findings"])
        assert all(f in inspect_proposal(proposal, request, context)["findings"] for f in failed["findings"])
    base, _ = proposal_pair(tmp_path)
    for change, code, location in (({"dataset_ref": None}, "missing_information", "request.dataset_ref"),
                                  ({"inputs": [None]}, "invalid_input", "request.inputs.0"),
                                  ({"task_id": "unknown"}, "invalid_identity", "task_id"),
                                  ({"missing_information": ["units"]}, "declared_missing_information", "request")):
        result = prepare_proposal(dict(base, **change))
        assert {"code": code, "location": location} in result["findings"]
    with pytest.raises(ResearchError):
        prepare_proposal(request, producer="rdagent")


def test_proposal_context_malformed_and_same_name(tmp_path):
    request, proposal = proposal_pair(tmp_path)
    ref = dict(source_id="s", locator=dict(line=1))
    request.update(source_refs=[ref], experience_query={"problem": "flow", "terms": ["flow"]})
    proposal["candidates"][0]["source_refs"] = [ref]
    context = dict(ok=True, result=dict(schema_version="factor_research_experience_v1", query=request["experience_query"], entries=[dict(source_ref=ref)]))
    assert inspect_proposal(proposal, request)["inspection_status"] == "requires_revision"
    other = dict(proposal["candidates"][0], local_id="two", formulation="-flow")
    proposal["candidates"].append(other)
    assert len(inspect_proposal(proposal, request, context)["candidates"]) == 2
    assert inspect_proposal(proposal, request, context)["inspection_status"] == "reviewable"
    for key, value in [("research_role", {}), ("inputs", [None]), ("horizons", []), ("artifact_root", "https://remote")]:
        assert inspect_proposal(proposal, dict(request, **{key: value}), context)["inspection_status"] == "requires_revision"
    for altered in (dict(proposal, generation_status="failed", candidates=[], failure_reason="provider unavailable"),
                    dict(proposal, candidates=[other, other]), dict(proposal, auto_run=True)):
        assert inspect_proposal(altered, request, context)["inspection_status"] == "requires_revision"
    context["result"]["entries"] = []
    assert any(f["code"] == "source_not_in_context" for f in inspect_proposal(proposal, request, context)["findings"])


def test_drift_duplicates_and_script_link(tmp_path, monkeypatch):
    import stat
    from types import SimpleNamespace

    text = tmp_path / "knowledge.md"
    text.write_text("flow failed on old qlib_bin", encoding="utf-8")
    sources = [dict(source_id=i, kind="knowledge_text", path=str(text)) for i in ("a", "b")]
    assert experience(query(sources))["matched_count"] == 1
    original = Path.stat
    calls = []

    def changing(path, **kwargs):
        result = original(path, **kwargs)
        if path == text:
            calls.append(1)
            return SimpleNamespace(st_mode=result.st_mode, st_size=result.st_size, st_mtime_ns=len(calls))
        return result

    with monkeypatch.context() as patch:
        patch.setattr(Path, "stat", changing)
        assert "source_changed" in experience(query(sources[:1]))["sources"][0]["reasons"]
    request, proposal = proposal_pair(tmp_path)
    proposal["candidates"][0].update(implementation="script_available", script_ref=str(text))
    monkeypatch.setattr(Path, "lstat", lambda _: SimpleNamespace(st_mode=stat.S_IFLNK, st_file_attributes=0))
    assert inspect_proposal(proposal, request)["inspection_status"] == "requires_revision"
