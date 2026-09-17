from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.services.factor_research.models import ResearchError
from backend.services.factor_research.rdagent_salvage import (
    audit_legacy_factor_code,
    classify_catalog,
    evaluation_name,
    group_candidates,
    inspect_factor_code,
    iter_central_export,
    iter_node_pickle_source,
    prepare_salvage_evaluation,
    prepare_source_record,
    render_legacy_adapter,
    run_salvage,
    run_salvage_metric_evaluation,
    run_salvage_precheck,
    run_salvage_reference_correlation,
)
from scripts.factor_research import dispatch, parser


FACTOR_A = """import pandas as pd

def calculate_alpha(data):
    return data[\"close\"].pct_change()
"""

FACTOR_A_FORMATTED = """import pandas as pd\r\n\r\ndef calculate_alpha(data):   \r\n    return data[\"close\"].pct_change()\r\n"""

FACTOR_B = """def calculate_alpha(data):
    return data[\"close\"].rolling(5).mean()
"""

LEGACY_FACTOR = '''import pandas as pd
import numpy as np

def calculate_legacy_alpha():
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()
    result = pd.DataFrame(index=df.index)
    result["legacy_alpha"] = df["close"].groupby(level="instrument").pct_change()
    result.to_hdf("result.h5", key="data", mode="w")
    return result

if __name__ == "__main__":
    calculate_legacy_alpha()
'''


def source(name: str, code: str, *, node: str = "node-a", task: str = "task-1") -> dict:
    return prepare_source_record({
        "record_type": "factor_source",
        "source_node": node,
        "source_kind": "fixture",
        "task_id": task,
        "loop_id": 1,
        "workspace_key": f"workspace_0/{name}.py",
        "factor_name": name,
        "code_text": code,
        "source_path": f"{task}/{name}.pkl",
    })


def test_inspection_never_executes_candidate_code(tmp_path: Path) -> None:
    marker = tmp_path / "executed"
    code = f'''Path({str(marker)!r}).write_text("bad")
def calculate_safe(data):
    return data
'''
    result = inspect_factor_code(code)
    assert result["valid"] is True
    assert result["entries"] == ["calculate_safe"]
    assert not marker.exists()


@pytest.mark.parametrize(
    ("code", "reason"),
    [("def broken(:\n pass", "python_ast_invalid"), ("class Model: pass", "factor_entry_missing")],
)
def test_inspection_reports_typed_unavailable(code: str, reason: str) -> None:
    result = inspect_factor_code(code)
    assert result["valid"] is False
    assert result["reason"] == reason


def test_grouping_is_order_invariant_and_preserves_name_conflict() -> None:
    records = [
        source("Alpha", FACTOR_A, node="wsl", task="task-1"),
        source("AlphaCopy", FACTOR_A_FORMATTED, node="node1", task="task-2"),
        source("Alpha", FACTOR_B, node="node1", task="task-3"),
    ]
    forward = group_candidates(records)
    reverse = group_candidates(reversed(records))
    assert forward == reverse
    assert len(forward) == 2
    assert sorted(group["member_count"] for group in forward) == [1, 2]
    assert all(group["source_name_conflicts"] for group in forward)


def test_grouping_has_a_stable_representative_for_case_variants() -> None:
    records = [source("factor_name", FACTOR_A), source("Factor_Name", FACTOR_A, task="task-2")]
    assert group_candidates(records) == group_candidates(reversed(records))
    assert group_candidates(records)[0]["representative_name"] == "Factor_Name"


def test_catalog_classification_separates_exact_conflict_absent_and_unavailable() -> None:
    exact = source("Existing", FACTOR_A)
    conflict = source("SameName", FACTOR_B, task="task-2")
    absent = source("NewName", "def calculate_new(data):\n return data\n", task="task-3")
    groups = group_candidates([exact, conflict, absent])
    catalog = [{"id": 10, "factor_name": "Existing", "factor_name_normalized": "existing",
                "ast_sha256": exact["ast_sha256"], "code_sha256": exact["code_sha256"], "source": "official"},
               {"id": 12, "factor_name": "ExistingAlias", "factor_name_normalized": "existingalias",
                "ast_sha256": exact["ast_sha256"], "code_sha256": exact["code_sha256"], "source": "official"},
               {"id": 11, "factor_name": "SameName", "factor_name_normalized": "samename",
                "ast_sha256": "other", "code_sha256": "other", "source": "official"}]
    classify_catalog(groups, catalog, True)
    status = {group["representative_name"]: group["catalog_status"] for group in groups}
    assert status == {
        "Existing": "catalog_exact_match",
        "NewName": "catalog_absent",
        "SameName": "catalog_name_conflict",
    }
    existing_group = next(group for group in groups if group["representative_name"] == "Existing")
    assert [match["id"] for match in existing_group["catalog_matches"]] == [10, 12]
    classify_catalog(groups, [], False)
    assert {group["catalog_status"] for group in groups} == {"catalog_unavailable"}


def test_central_export_only_reads_factor_directory(tmp_path: Path) -> None:
    task = tmp_path / "task-1"
    (task / "factors").mkdir(parents=True)
    (task / "factors" / "Alpha.py").write_text(FACTOR_A, encoding="utf-8")
    (task / "model.py").write_text("class Model: pass\n", encoding="utf-8")
    (task / "factor_order.json").write_text(
        json.dumps({"dynamic_factors": ["Alpha"]}), encoding="utf-8",
    )
    rows = list(iter_central_export(tmp_path))
    assert len(rows) == 1
    assert rows[0]["factor_name"] == "Alpha"
    assert rows[0]["exported_dynamic_factor"] is True


def test_run_salvage_closes_denominators_and_refuses_overwrite(tmp_path: Path) -> None:
    task = tmp_path / "sources" / "task-1" / "factors"
    task.mkdir(parents=True)
    (task / "Alpha.py").write_text(FACTOR_A, encoding="utf-8")
    (task / "NotFactor.py").write_text("class Model: pass\n", encoding="utf-8")
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"factors": []}), encoding="utf-8")
    artifact_root = tmp_path / "artifacts"
    summary = run_salvage({
        "sources": [{"kind": "central_export", "node_id": "central", "root": str(tmp_path / "sources")}],
        "catalog_snapshot": str(catalog_path),
    }, artifact_root)
    assert summary["source_objects"] == 2
    assert summary["grouped_members"] == 1
    assert summary["unavailable"] == 1
    assert summary["source_denominator_closed"] is True
    assert summary["source_scan_complete"] is True
    assert summary["group_denominator_closed"] is True
    assert summary["review_candidates"] == 1
    assert summary["review_catalog_status_counts"] == {"catalog_absent": 1}
    assert summary["review_source_presence"] == {"central": 1}
    assert summary["review_with_exported_dynamic_evidence"] == 0
    assert summary["database_writes"] == 0
    assert {path.name for path in artifact_root.iterdir()} == {
        "source_inventory.jsonl", "candidate_groups.jsonl", "review_candidates.jsonl",
        "unavailable.jsonl", "dry_run_summary.json",
    }
    review_row = json.loads((artifact_root / "review_candidates.jsonl").read_text(encoding="utf-8"))
    assert "code_text" not in review_row
    with pytest.raises(ResearchError, match="Artifact root"):
        run_salvage({"sources": [{"kind": "central_export", "node_id": "central",
                                   "root": str(tmp_path / "sources")}]}, artifact_root)


def test_run_salvage_refuses_artifact_root_inside_git_repository(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    repository.mkdir()
    (repository / ".git").write_text("gitdir: elsewhere\n", encoding="utf-8")
    with pytest.raises(ResearchError, match="outside every Git repository"):
        run_salvage({"sources": [{"kind": "central_export", "node_id": "central",
                                   "root": str(tmp_path / "sources")}]}, repository / "artifacts")


def test_missing_source_is_not_reported_as_a_complete_scan(tmp_path: Path) -> None:
    summary = run_salvage({
        "sources": [{"kind": "central_export", "node_id": "central", "root": str(tmp_path / "missing")}],
    }, tmp_path / "artifacts")
    assert summary["source_scan_complete"] is False
    assert summary["source_status"] == {"central": {"factor_sources": 0, "unavailable": 1}}


def test_salvage_cli_is_offline_and_does_not_require_database_configuration(tmp_path: Path) -> None:
    source_root = tmp_path / "sources" / "task-1" / "factors"
    source_root.mkdir(parents=True)
    (source_root / "Alpha.py").write_text(FACTOR_A, encoding="utf-8")
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps({
        "sources": [{"kind": "central_export", "node_id": "central", "root": str(tmp_path / "sources")}],
    }), encoding="utf-8")
    args = parser().parse_args([
        "salvage", "--input", str(spec_path), "--artifact-root", str(tmp_path / "artifacts"), "--format", "json",
    ])
    result = dispatch(args)
    assert result["ok"] is True
    assert result["result"]["database_writes"] == 0
    assert result["result"]["catalog_status_counts"] == {"catalog_unavailable": 1}


@pytest.mark.parametrize("timeout", [0, True, 86401, "10"])
def test_node_extractor_rejects_invalid_timeout_before_process_launch(timeout: object) -> None:
    with pytest.raises(ResearchError, match="timeout_seconds"):
        list(iter_node_pickle_source({
            "kind": "wsl_pickle", "node_id": "wsl", "distro": "Ubuntu",
            "python": "/python", "root": "/logs", "timeout_seconds": timeout,
        }))


def test_node_extractor_rejects_ssh_option_injection_as_host() -> None:
    with pytest.raises(ResearchError, match="host"):
        list(iter_node_pickle_source({
            "kind": "ssh_pickle", "node_id": "node1", "host": "-oProxyCommand",
            "python": "/python", "root": "/logs",
        }))


def test_legacy_audit_accepts_read_only_contract_and_rejects_side_effects() -> None:
    accepted = audit_legacy_factor_code(LEGACY_FACTOR)
    inspected = inspect_factor_code(LEGACY_FACTOR)
    assert accepted == {
        "compatible": True,
        "reasons": [],
        "data_files": ["daily_pv.h5"],
        "entry": "calculate_legacy_alpha",
        "code_sha256": inspected["code_sha256"],
        "ast_sha256": inspected["ast_sha256"],
    }
    unsafe = LEGACY_FACTOR.replace(
        "df = pd.read_hdf", 'open("unexpected.txt", "w")\n    df = pd.read_hdf',
    )
    assert audit_legacy_factor_code(unsafe)["reasons"] == ["side_effect_or_dynamic_execution"]
    dynamic = LEGACY_FACTOR.replace('"daily_pv.h5"', "input_path", 1)
    assert "data_dependency_unsupported" in audit_legacy_factor_code(dynamic)["reasons"]


def test_rendered_legacy_adapter_respects_scope_and_does_not_mutate_input(tmp_path: Path) -> None:
    import subprocess
    import sys

    import pandas as pd

    data_dir = tmp_path / "data"
    work_dir = tmp_path / "work"
    data_dir.mkdir()
    work_dir.mkdir()
    index = pd.MultiIndex.from_product(
        [pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]), ["000001.SZ", "000002.SZ"]],
        names=["datetime", "instrument"],
    )
    source_path = data_dir / "daily_pv.h5"
    pd.DataFrame({"close": range(1, 7)}, index=index).to_hdf(
        source_path, key="data", mode="w", format="table", data_columns=True,
    )
    before = source_path.stat().st_mtime_ns
    name = evaluation_name("Legacy Alpha", inspect_factor_code(LEGACY_FACTOR)["ast_sha256"])
    script = work_dir / "adapter.py"
    script.write_text(
        render_legacy_adapter(
            code_text=LEGACY_FACTOR,
            entry="calculate_legacy_alpha",
            name=name,
            data_files=["daily_pv.h5"],
        ),
        encoding="utf-8",
    )
    output = work_dir / "values.h5"
    subprocess.run(
        [
            sys.executable, str(script), "--data-dir", str(data_dir), "--output", str(output),
            "--start-date", "2024-01-03", "--end-date", "2024-01-04",
            "--instruments", '["000001.SZ"]',
        ],
        cwd=work_dir,
        check=True,
    )
    values = pd.read_hdf(output, key="data")
    assert list(values.columns) == [name]
    assert values.index.tolist() == [
        (pd.Timestamp("2024-01-03"), "000001.SZ"),
        (pd.Timestamp("2024-01-04"), "000001.SZ"),
    ]
    assert source_path.stat().st_mtime_ns == before
    assert not (work_dir / "daily_pv.h5").exists()
    assert not (work_dir / "result.h5").exists()


def test_prepare_salvage_evaluation_materializes_audited_candidate(tmp_path: Path) -> None:
    source_root = tmp_path / "sources" / "task-1" / "factors"
    source_root.mkdir(parents=True)
    (source_root / "LegacyAlpha.py").write_text(LEGACY_FACTOR, encoding="utf-8")
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"factors": []}), encoding="utf-8")
    inventory = tmp_path / "inventory"
    run_salvage({
        "sources": [{"kind": "central_export", "node_id": "central", "root": str(tmp_path / "sources")}],
        "catalog_snapshot": str(catalog),
    }, inventory)
    groups_path = inventory / "candidate_groups.jsonl"
    group = json.loads(groups_path.read_text(encoding="utf-8"))
    group["ast_sha256"] = "f" * 64  # AST dumps vary by Python version; source identity is portable.
    group["candidate_id"] = "ast:" + group["ast_sha256"]
    groups_path.write_text(json.dumps(group) + "\n", encoding="utf-8")
    review_path = inventory / "review_candidates.jsonl"
    review = json.loads(review_path.read_text(encoding="utf-8"))
    review["candidate_id"] = group["candidate_id"]
    review_path.write_text(json.dumps(review) + "\n", encoding="utf-8")
    prepared_root = tmp_path / "prepared"
    summary = prepare_salvage_evaluation(inventory, prepared_root)
    assert summary["inventory_candidates"] == 1
    assert summary["prepared_candidates"] == 1
    assert summary["unavailable_candidates"] == 0
    assert summary["denominator_closed"] is True
    assert summary["candidate_execution_count"] == 0
    row = json.loads((prepared_root / "evaluation_candidates.jsonl").read_text(encoding="utf-8"))
    assert row["candidate_id"].startswith("ast:")
    assert Path(row["script"]).is_file()
    assert row["data_files"] == ["daily_pv.h5"]


def test_salvage_prepare_cli_is_offline(tmp_path: Path) -> None:
    inventory = tmp_path / "inventory"
    inventory.mkdir()
    (inventory / "candidate_groups.jsonl").write_text("", encoding="utf-8")
    (inventory / "review_candidates.jsonl").write_text("", encoding="utf-8")
    args = parser().parse_args([
        "salvage-prepare", "--inventory-root", str(inventory),
        "--artifact-root", str(tmp_path / "prepared"), "--format", "json",
    ])
    result = dispatch(args)
    assert result["ok"] is True
    assert result["result"]["candidate_execution_count"] == 0
    assert result["result"]["database_writes"] == 0


def test_salvage_precheck_executes_bounded_scope_without_value_conclusion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import subprocess

    import pandas as pd

    source_root = tmp_path / "sources" / "task-1" / "factors"
    source_root.mkdir(parents=True)
    (source_root / "LegacyAlpha.py").write_text(LEGACY_FACTOR, encoding="utf-8")
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"factors": []}), encoding="utf-8")
    inventory = tmp_path / "inventory"
    run_salvage({
        "sources": [{"kind": "central_export", "node_id": "central", "root": str(tmp_path / "sources")}],
        "catalog_snapshot": str(catalog),
    }, inventory)
    prepared = tmp_path / "prepared"
    prepare_salvage_evaluation(inventory, prepared)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    index = pd.MultiIndex.from_product(
        [pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]), ["000001.SZ", "000002.SZ"]],
        names=["datetime", "instrument"],
    )
    pd.DataFrame({"close": range(1, 7)}, index=index).to_hdf(
        data_dir / "daily_pv.h5", key="data", mode="w", format="table", data_columns=True,
    )
    commands = []
    real_run = subprocess.run

    def capture_command(command, **kwargs):
        commands.append(command)
        return real_run(command, **kwargs)

    monkeypatch.setattr(
        "backend.services.factor_research.rdagent_salvage.subprocess.run",
        capture_command,
    )
    summary = run_salvage_precheck({
        "prepared_root": str(prepared),
        "data_dir": str(data_dir),
        "start_date": "2024-01-03",
        "end_date": "2024-01-04",
        "instruments": ["000001.SZ"],
        "timeout_seconds": 30,
    }, tmp_path / "precheck")
    assert summary["requested_candidates"] == 1
    assert summary["available_candidates"] == 1
    assert summary["unavailable_candidates"] == 0
    assert summary["denominator_closed"] is True
    assert summary["value_conclusions"] == 0
    assert "--instruments-file" in commands[0]
    assert "--instruments" not in commands[0]
    assert json.loads((tmp_path / "precheck" / "scope_instruments.json").read_text(encoding="utf-8")) == [
        "000001.SZ",
    ]


def test_salvage_precheck_rejects_unknown_candidate_selection(tmp_path: Path) -> None:
    prepared = tmp_path / "prepared"
    prepared.mkdir()
    (prepared / "evaluation_candidates.jsonl").write_text("", encoding="utf-8")
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    with pytest.raises(ResearchError, match="unknown identities"):
        run_salvage_precheck({
            "prepared_root": str(prepared),
            "data_dir": str(data_dir),
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "instruments": ["000001.SZ"],
            "timeout_seconds": 30,
            "candidate_ids": ["ast:" + "a" * 64],
        }, tmp_path / "precheck")


def test_salvage_metric_evaluation_reuses_context_and_closes_denominator(tmp_path: Path) -> None:
    import pandas as pd

    prepared = tmp_path / "prepared"
    values_root = tmp_path / "values-root"
    qlib = tmp_path / "qlib"
    prepared.mkdir()
    values_root.mkdir()
    qlib.mkdir()
    candidate_id = "ast:" + "a" * 64
    name = "rdg_candidate_aaaaaaaaaa"
    (prepared / "evaluation_candidates.jsonl").write_text(json.dumps({
        "candidate_id": candidate_id,
        "evaluation_name": name,
        "representative_name": "Candidate",
        "catalog_status": "catalog_absent",
    }) + "\n", encoding="utf-8")
    index = pd.MultiIndex.from_product(
        [pd.to_datetime(["2024-01-02", "2024-01-03"]), ["000001.SZ", "000002.SZ"]],
        names=["datetime", "instrument"],
    )
    value_path = values_root / "values.h5"
    pd.DataFrame({name: [1.0, 2.0, 2.0, 3.0]}, index=index).to_hdf(value_path, key="data")
    (values_root / "precheck_results.jsonl").write_text(json.dumps({
        "candidate_id": candidate_id,
        "evaluation_name": name,
        "status": "available",
        "values": str(value_path),
    }) + "\n", encoding="utf-8")
    prepare_calls = []

    def fake_prepare(**kwargs):
        prepare_calls.append(kwargs)
        dates = pd.DatetimeIndex(["2024-01-02", "2024-01-03"])
        columns = ["000001.SZ", "000002.SZ"]
        panel = pd.DataFrame([[1.0, 2.0], [2.0, 3.0]], index=dates, columns=columns)
        return {
            "close_unstacked": panel,
            "st_pit_eligible_mask": panel.notna(),
            "fwd_ret_mats": {"1d": panel * 0.01},
        }

    def fake_compute(factor_name, values, ctx, *, evaluation_windows, include_horizon_metrics):
        assert factor_name == name
        assert include_horizon_metrics is True
        assert "full" in evaluation_windows
        return {"factor_results": [{"rank_ic_mean": 0.1}], "undefined": float("nan")}

    summary = run_salvage_metric_evaluation({
        "prepared_root": str(prepared),
        "value_roots": [str(values_root)],
        "qlib_bin_path": str(qlib),
        "universe_key": "aistock_equity_pit_canonical_v2",
        "instruments": ["000001.SZ", "000002.SZ"],
        "read_start": "2024-01-02",
        "read_end": "2024-01-03",
        "signal_start": "2024-01-02",
        "signal_end": "2024-01-03",
        "cutoff": "2024-01-03",
    }, tmp_path / "metrics", prepare=fake_prepare, compute=fake_compute)
    assert len(prepare_calls) == 1
    assert summary["requested_candidates"] == summary["evaluated_candidates"] == 1
    assert summary["unavailable_candidates"] == 0
    assert summary["denominator_closed"] is True
    assert summary["nonfinite_metric_values_as_null"] == 1
    row = json.loads((tmp_path / "metrics" / "metric_results.jsonl").read_text(encoding="utf-8"))
    assert row["metrics"]["undefined"] is None
    assert row["catalog_status"] == "catalog_absent"


def test_salvage_reference_correlation_is_bounded_and_closes_pairs(tmp_path: Path) -> None:
    import numpy as np
    import pandas as pd

    prepared = tmp_path / "prepared"
    values_root = tmp_path / "values-root"
    prepared.mkdir()
    values_root.mkdir()
    candidates = [
        ("ast:" + "a" * 64, "rdg_candidate_a_aaaaaaaaaa"),
        ("ast:" + "b" * 64, "rdg_candidate_b_bbbbbbbbbb"),
    ]
    (prepared / "evaluation_candidates.jsonl").write_text(
        "".join(json.dumps({
            "candidate_id": candidate_id,
            "evaluation_name": name,
            "representative_name": name,
        }) + "\n" for candidate_id, name in candidates),
        encoding="utf-8",
    )
    dates = pd.bdate_range("2024-01-02", periods=4)
    symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
    index = pd.MultiIndex.from_product([dates, symbols], names=["datetime", "instrument"])
    result_rows = []
    for position, (candidate_id, name) in enumerate(candidates):
        path = values_root / f"candidate-{position}.h5"
        values = np.tile(np.arange(3, dtype=float) + position, 4)
        pd.DataFrame({name: values}, index=index).to_hdf(path, key="data")
        result_rows.append({
            "candidate_id": candidate_id,
            "evaluation_name": name,
            "status": "available",
            "values": str(path),
        })
    (values_root / "precheck_results.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in result_rows), encoding="utf-8",
    )
    reference = tmp_path / "reference.parquet"
    pd.DataFrame({"value": np.tile(np.arange(3, dtype=float), 4)}, index=index).to_parquet(reference)
    summary = run_salvage_reference_correlation({
        "prepared_root": str(prepared),
        "value_roots": [str(values_root)],
        "reference_value_artifacts": {"official_reference": str(reference)},
        "windows": {"full": {"start": "2024-01-02", "end": "2024-01-05"}},
        "candidate_batch_size": 1,
        "reference_batch_size": 1,
        "correlation_min_stocks": 3,
        "correlation_min_effective_days": 2,
        "correlation_half_life": 2,
        "top_k": 1,
    }, tmp_path / "correlations")
    assert summary["candidate_count"] == 2
    assert summary["requested_pairs"] == summary["available_pairs"] == 2
    assert summary["unavailable_pairs"] == 0
    assert summary["denominator_closed"] is True
    assert summary["reference_reference_pairs_computed"] == 0
    rows = [json.loads(line) for line in (
        tmp_path / "correlations" / "reference_correlation_results.jsonl"
    ).read_text(encoding="utf-8").splitlines()]
    assert all(row["windows"]["full"]["top_absolute_correlations"][0]["reference"] ==
               "official_reference" for row in rows)
