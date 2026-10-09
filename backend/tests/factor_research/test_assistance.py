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
    def run(*args, status=0):
        result = subprocess.run([sys.executable, "-c", program, *args], cwd=root, capture_output=True, text=True)
        assert result.returncode == status, result.stderr
        return result.stdout
    request, proposal = proposal_pair(tmp_path)
    original = dump(tmp_path / "original.json", request)
    for args, key, expected in [(["experience", "--input", file], "retrieval_status", "unavailable"),
                                (["proposal-inspect", "--request", original, "--input", dump(tmp_path / "proposal.json", proposal)], "inspection_status", "reviewable"),
                                (["proposal-prepare", "--input", original, "--producer", "claude"], "prepare_status", "prepared")]:
        pack = json.loads(run(*args, "--format", "json"))["result"]
        assert pack[key] == expected
        assert not any(pack.get(k) for k in ("generation_performed", "model_call_performed", "execution_performed"))
    assert pack["producer_target"] == "claude" and "--target" in run("memory-search", "--help")
    invalid = run("memory-search", "--input", file, "--env-file", "missing.env", "--target", "dev", "--format", "json", status=1)
    assert json.loads(invalid)["error"]["code"] == "invalid_request"


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
    assert json.dumps([request, context], sort_keys=True) == before
    context["result"]["entries"].append(dict(entry, observation=dict(text="contradiction", truncated=False)))
    for entries in (context["result"]["entries"], context["result"]["entries"][::-1]):
        context["result"]["entries"] = entries
        failed = prepare_proposal(request, context)
        assert failed["prepare_status"] == "requires_revision" and failed["selected_experience"] == []
        assert any(f["code"] == "source_ref_ambiguous" for f in failed["findings"])
        assert all(f in inspect_proposal(proposal, request, context)["findings"] for f in failed["findings"])
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
    for key, value in [("research_role", {}), ("inputs", [None]), ("horizons", []), ("artifact_root", "https://remote"),
                       ("dataset_ref", None), ("task_id", "unknown"), ("missing_information", ["units"])]:
        assert inspect_proposal(proposal, dict(request, **{key: value}), context)["inspection_status"] == "requires_revision"
        assert prepare_proposal(dict(request, **{key: value}), context)["prepare_status"] == "requires_revision"
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


def test_memory_unknown_conflict_literal_paging_and_old_context():
    from backend.services.factor_research.memory import search_rows, validate_query

    request = dict(schema_version="factor_research_memory_query_v1", query=dict(problem="反转", terms=["FLOW", "flow", "%_"]),
                   filters={"horizons": ["20d"]}, include_unknown=True, limit=2)
    rows = [dict(task_id=str(uuid4()), record_id=str(uuid4()), revision=1, summary="flow %_",
                 note=note, contexts=contexts) for note, contexts in (
                     ({"observation": "原观察", "conditions": {"horizons": ["20d"]}}, []),
                     (None, []), (42, []),
                     ({"conditions": {"horizons": ["1d"]}}, []),
                     ({"conditions": {"horizons": ["20d"]}}, [{"horizons": ["1d"]}]))]
    query_value = validate_query(request)
    found = search_rows(iter(rows), query_value, "dev")
    assert found["matched_count"] == 4 and found["unknown_filter_count"] == 3
    assert found["next_offset"] == 2 and len(found["entries"]) == 2
    assert found["entries"] == search_rows(iter(rows[::-1]), query_value, "dev")["entries"]
    strict = search_rows(iter(rows), validate_query(dict(request, include_unknown=False)), "dev")
    assert strict["matched_count"] == 1 and strict["unknown_filter_count"] == 3
    assert len(strict["entries"][0]["match_basis"]["terms"]) == 2
    all_rows = search_rows(iter(rows), validate_query(dict(request, limit=20)), "dev")["entries"]
    assert any("invalid_note" in e["missing_information"] for e in all_rows)
    assert any("condition_conflict:horizons" in e["missing_information"] for e in all_rows)
    past = search_rows(iter(rows), validate_query(dict(request, offset=30)), "dev")
    assert past["matched_count"] == 4 and not past["entries"] and past["match_status"] == "matched"
    explanation = dict(rows[0], summary="", note={"interpretation": "flow"})
    assert search_rows(iter([explanation]), query_value, "dev")["matched_count"] == 1
    empty = search_rows(iter([explanation]), validate_query(dict(request, query={"problem": "keys", "terms": ["summary"]})), "dev")
    assert empty["match_status"] == "no_match_in_read_scope"
    for change in ({"limit": True}, {"filters": {"horizons": ["2d"]}}, {"query": {"problem": "x", "terms": []}}):
        with pytest.raises(ResearchError):
            validate_query(dict(request, **change))


def test_memory_corrections_survive_proposal_selection(tmp_path):
    request, proposal = proposal_pair(tmp_path)
    task = str(uuid4())
    def entry(revision):
        return dict(source_ref=dict(source_id="aistock_research:dev", locator=dict(task_id=task, record_id=str(uuid4()), revision=revision)),
                    observation={"text": "old result"}, related_refs=[], relations_complete=True)
    old, correction, other = [entry(i) for i in (1, 2, 3)]
    old["related_refs"] = [correction["source_ref"]]
    correction["related_refs"] = [old["source_ref"]]
    request.update(source_refs=[old["source_ref"]], experience_query={"terms": ["old"], "filters": {"horizons": ["20d"]}})
    context = dict(ok=True, result=dict(schema_version="factor_research_experience_v1", query=request["experience_query"],
                   entries=[old, other], related_entries=[correction], scope="research_database", relations_complete=True))
    pack = prepare_proposal(request, context)
    assert pack["prepare_status"] == "prepared"
    assert pack["experience_scope"]["related_entries"] == [correction]
    assert any("竞争解释" in text for text in pack["instructions"])
    proposal["candidates"][0]["source_refs"] = [correction["source_ref"]]
    assert inspect_proposal(proposal, request, context)["inspection_status"] == "requires_revision"
    request["source_refs"] = [correction["source_ref"]]
    assert prepare_proposal(request, context)["selected_experience"] == [correction]
    correction["source_ref"]["locator"]["task_id"] = str(uuid4())
    request["source_refs"] = [old["source_ref"]]
    assert prepare_proposal(request, context)["prepare_status"] == "requires_revision"
