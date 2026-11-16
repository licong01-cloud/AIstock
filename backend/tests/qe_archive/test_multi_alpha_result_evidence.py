from __future__ import annotations

from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from backend.services.multi_alpha.combine_backtest import (
    ingest_enhanced_metrics, parse_request, persisted_backtest_config_for,
)
from backend.services.qe_archive.models import sha256_json
from backend.services.qe_archive.multi_alpha_result_evidence import (
    load_selected_baseline_evidence, require_archive_preview,
)
from backend.services.qe_archive.handlers.multi_alpha_combine_archive_handler import MultiAlphaCombineArchiveHandler


@pytest.fixture
def sealed_baseline(tmp_path):
    request = parse_request({
        "roster": [{"leg_id": "AF", "seed_run_ids": ["s42", "s2024", "s2026"]}],
        "weighting_schemes": [], "walk_forward": {"enabled": False},
        "prediction_task_selection": {"include_baseline": True, "include_loo": False, "baseline_only": True},
        "oos_start": "2026-04-01", "oos_end": "2026-04-02",
        "backtest_config": {"node_id": "wsl2-5080"},
    })
    raw = {"absolute_returns": {"cagr": -.1, "max_drawdown": -.2, "sharpe": -.3}}
    path = tmp_path / "qlib_results_enhanced.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    metrics = ingest_enhanced_metrics(path)
    metrics.update(prediction_store_manifest=None, pred_persisted=False)
    manifest = {
        "run_id": "macb_1", "child_id": "c1", "attempt_id": "a1", "node_id": "wsl2-5080",
        "qe_task_id": "remote1", "qe_loop_id": "Loop1", "artifact_manifest_hash": "published",
        "raw_result_file": path.name, "raw_result_hash": sha256_json(raw), "metrics_hash": sha256_json(metrics),
        "result_window": {"effective_start": "2026-04-01", "effective_end": "2026-04-02", "trading_date_count": 2},
        "prediction_store_manifest": None,
    }
    manifest["prediction_window"] = deepcopy(manifest["result_window"])
    manifest["manifest_hash"] = sha256_json(manifest)
    (tmp_path / "result_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    source = {"run": {"id": "macb_1", "status": "succeeded", "roster_hash": "fixture",
                       "roster_json": [{"leg_id": "AF", "seed_run_ids": []}],
                       "backtest_config_json": persisted_backtest_config_for(request),
                       "oos_start": request.oos_start, "oos_end": request.oos_end},
              "scheme_results": [],
              "children": [{"child_id": "c1", "run_id": "macb_1", "child_kind": "baseline",
                            "child_key": "baseline:AF", "status": "succeeded", "selected_attempt_id": "a1"}],
              "attempts": [{"attempt_id": "a1", "child_id": "c1", "run_id": "macb_1", "node_id": "wsl2-5080",
                            "qe_task_id": "remote1", "qe_loop_id": "Loop1", "status": "succeeded",
                            "result_manifest_json": manifest, "result_manifest_hash": sha256_json(manifest)}]}
    artifacts = SimpleNamespace(workspace=tmp_path, artifact_manifest={"manifest_hash": "published"})
    adapter = SimpleNamespace(load_published_artifacts=lambda **_: artifacts,
                              load_collected_metrics=lambda _: ingest_enhanced_metrics(path))
    return source, adapter, tmp_path


def test_real_hash_only_manifest_yields_negative_metrics_without_mutating_identity(sealed_baseline):
    source, adapter, _ = sealed_baseline
    before = deepcopy(source)
    evidence = load_selected_baseline_evidence(source, adapter_factory=lambda: adapter)
    assert evidence["metrics"]["cagr"] == -.1
    assert evidence["metrics"]["sharpe"] == -.3
    assert evidence["new_fit_predict_backtest"] == 0
    assert source == before


@pytest.mark.parametrize("mutation,reason", [
    (lambda s: s["children"][0].update(selected_attempt_id="missing"), "selected_attempt_missing"),
    (lambda s: s["children"].append(deepcopy(s["children"][0])), "exact_single_child"),
    (lambda s: s["attempts"][0].update(status="failed"), "remote_attempt_mismatch"),
    (lambda s: s["attempts"][0].update(result_manifest_hash="bad"), "manifest_hash_mismatch"),
    (lambda s: s["children"][0].update(run_id="other"), "child_identity_mismatch"),
])
def test_rejects_wrong_selected_attempt_and_frozen_identity(sealed_baseline, mutation, reason):
    source, adapter, _ = sealed_baseline
    mutation(source)
    with pytest.raises(ValueError, match=reason):
        load_selected_baseline_evidence(source, adapter_factory=lambda: adapter)


@pytest.mark.parametrize("name", ["qlib_results_enhanced.json", "result_manifest.json"])
def test_missing_sealed_file_never_falls_back(sealed_baseline, name):
    source, adapter, directory = sealed_baseline
    (directory / name).unlink()
    with pytest.raises(FileNotFoundError):
        load_selected_baseline_evidence(source, adapter_factory=lambda: adapter)


def test_changed_raw_result_is_not_accepted(sealed_baseline):
    source, adapter, directory = sealed_baseline
    (directory / "qlib_results_enhanced.json").write_text('{"absolute_returns":{"cagr":9}}', encoding="utf-8")
    with pytest.raises(ValueError, match="raw_result_hash_mismatch"):
        load_selected_baseline_evidence(source, adapter_factory=lambda: adapter)


def test_changed_metric_reader_is_not_accepted(sealed_baseline):
    source, adapter, _ = sealed_baseline
    adapter.load_collected_metrics = lambda _: {"cagr": 9}
    with pytest.raises(ValueError, match="metrics_hash_mismatch"):
        load_selected_baseline_evidence(source, adapter_factory=lambda: adapter)


def test_shortened_window_is_not_accepted_even_when_manifest_is_resealed(sealed_baseline):
    source, adapter, directory = sealed_baseline
    manifest = source["attempts"][0]["result_manifest_json"]
    manifest["result_window"]["effective_end"] = "2026-04-01"
    manifest["manifest_hash"] = sha256_json({k: v for k, v in manifest.items() if k != "manifest_hash"})
    source["attempts"][0]["result_manifest_hash"] = sha256_json(manifest)
    (directory / "result_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="frozen_window_mismatch"):
        load_selected_baseline_evidence(source, adapter_factory=lambda: adapter)


@pytest.mark.parametrize("report", [
    {"run_id": "r", "dry_run": True, "written": False, "skipped_reason": "qe_lifecycle_no_evaluable_result"},
    {"run_id": "other", "dry_run": True, "written": False},
    {"run_id": "r", "dry_run": False, "written": True},
])
def test_successful_http_payload_is_not_sufficient_for_preview(report):
    with pytest.raises(ValueError):
        require_archive_preview(report, "r")


def test_eligible_preview_makes_no_write():
    require_archive_preview({"run_id": "r", "dry_run": True, "written": False}, "r")


def test_handler_uses_verified_baseline_and_retains_evidence(sealed_baseline, monkeypatch):
    source, adapter, _ = sealed_baseline
    before = deepcopy(source)
    writes = []
    repository = SimpleNamespace(fetch_multi_alpha_combine_run=lambda _: source,
                                 archive_multi_alpha_bundle=lambda **kw: writes.append(kw) or {"rows_upserted": 1})
    handler = MultiAlphaCombineArchiveHandler(repository=repository,
        baseline_evidence_loader=lambda s: load_selected_baseline_evidence(s, adapter_factory=lambda: adapter))
    bundle = SimpleNamespace(archive_run={}, run_header={}, run_source={"metadata": {}}, legs=[],
                             leg_sources=[], schemes=[], loo=[], recovery_children=[], recovery_attempts=[],
                             stats={"run_id": "macb_1"})
    monkeypatch.setattr(handler, "build_bundle", lambda _: bundle)
    assert handler.archive_run("macb_1", dry_run=True)["written"] is False
    assert writes == []
    assert handler.archive_run("macb_1", dry_run=False)["written"] is True
    evidence = writes[0]["run_source"]["metadata"]["selected_baseline_result"]
    assert evidence["metrics"]["cagr"] == -.1 and evidence["attempt_id"] == "a1"
    assert writes[0]["schemes"] == [] and source == before


def test_zero_returns_remain_evaluable(sealed_baseline):
    from backend.services.qe_archive.asset_lifecycle import classify_qe_result, QEValueClass
    source, adapter, directory = sealed_baseline
    raw = {"absolute_returns": {"cagr": 0, "max_drawdown": 0, "sharpe": 0}}
    (directory / "qlib_results_enhanced.json").write_text(json.dumps(raw), encoding="utf-8")
    metrics = ingest_enhanced_metrics(directory / "qlib_results_enhanced.json")
    metrics.update(prediction_store_manifest=None, pred_persisted=False)
    manifest = source["attempts"][0]["result_manifest_json"]
    manifest.update(raw_result_hash=sha256_json(raw), metrics_hash=sha256_json(metrics))
    manifest["manifest_hash"] = sha256_json({k: v for k, v in manifest.items() if k != "manifest_hash"})
    source["attempts"][0]["result_manifest_hash"] = sha256_json(manifest)
    (directory / "result_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    evidence = load_selected_baseline_evidence(source, adapter_factory=lambda: adapter)
    assert evidence["metrics"]["cagr"] == 0
    assert classify_qe_result({"status": "succeeded", "metrics": evidence["metrics"]}).value_class != QEValueClass.X


def test_prediction_metadata_numbers_do_not_make_empty_financial_metrics_evaluable(sealed_baseline):
    source, _, _ = sealed_baseline
    repository = SimpleNamespace(fetch_multi_alpha_combine_run=lambda _: source)
    handler = MultiAlphaCombineArchiveHandler(repository=repository, baseline_evidence_loader=lambda _: {
        "attempt_id": "a1", "metrics": {"cagr": None, "sharpe": None, "max_drawdown": None,
            "calmar": None, "prediction_store_manifest": {"row_count": 10000}}})
    assert handler.archive_run("macb_1")["skipped_reason"] == "qe_lifecycle_no_evaluable_result"


@pytest.mark.parametrize("policy", ["SKIP", "smoke"])
def test_existing_exclusion_does_not_read_baseline_artifacts(sealed_baseline, policy):
    source, _, _ = sealed_baseline
    if policy == "SKIP":
        source["run"]["backtest_config_json"]["archive_policy"] = "SKIP"
    else:
        source["run"]["backtest_config_json"]["purpose"] = "smoke"
    def forbidden(_):
        raise AssertionError("excluded run must not load assets")
    repository = SimpleNamespace(fetch_multi_alpha_combine_run=lambda _: source)
    handler = MultiAlphaCombineArchiveHandler(repository=repository, baseline_evidence_loader=forbidden)
    assert handler.archive_run("macb_1")["skipped_reason"]
