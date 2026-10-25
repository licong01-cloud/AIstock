from copy import deepcopy
from datetime import date
import hashlib
import json
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.batch_inference_provider import AdvisoryBatchInferenceProvider, _workspace_identity
from backend.services.trading_core.errors import DataUnavailableError
from scripts.strategy_package_live_inference import _atomic_json


def _fixture(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "manifest.json").write_text('{"primary_assets":{}}', encoding="utf-8")
    (workspace / "factor_order.json").write_text("[]", encoding="utf-8")
    provider = AdvisoryBatchInferenceProvider(temporary_root=tmp_path / "bridge", repo_root=tmp_path)
    days, calls = (date(2026, 6, 2), date(2026, 6, 3)), []
    def execute(workspace, clocks, scope):
        calls.append(clocks)
        root = tmp_path / f"result-{len(calls)}"
        index = {"binding": "bound-code-input", "status": "FAILED", "reason_code": "source_unavailable", "results": []}
        day = clocks[0]
        path = root / "batch_dates" / day.decision_date.isoformat() / "scores.json"
        _atomic_json(path, {"ok": True, "scores": [], "universe_count": 100, "input_context": {}, "source_read_receipts": [],
                            "metadata": {"consumer_scope": "ADVISORY", "run_id": scope.run_id, "clocks": day.to_dict(), "binding": index["binding"]}})
        from backend.infra.wsl_qlib_runner import win_to_wsl_path
        index["results"].append({"trade_date": day.decision_date.isoformat(), "output_path": win_to_wsl_path(str(path)),
                                 "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        return root.resolve(), index
    monkeypatch.setattr(provider, "_execute", execute)
    return provider, SimpleNamespace(workspace_path=workspace), days, calls


def test_native_provider_batch_own_results_partial_failure_and_no_cross_run_reuse(tmp_path, monkeypatch):
    provider, workspace, days, calls = _fixture(tmp_path, monkeypatch)
    with provider.batch_dates(trade_dates=days, run_id="own", source_revision="frozen-catalog"):
        first = provider.run(workspace=workspace, trade_date=days[0], historical_read_only=True)
        first.metadata["clocks"]["data_cutoff"] = "tampered"
        assert provider.run(workspace=workspace, trade_date=days[0], historical_read_only=True).metadata["clocks"]["data_cutoff"] == days[0].isoformat()
        assert first.metadata["inference_batch_status"] == "FAILED" and len(calls) == 1
        with pytest.raises(DataUnavailableError) as incomplete:
            provider.run(workspace=workspace, trade_date=days[1], historical_read_only=True)
        assert incomplete.value.context["reason_code"] == "source_unavailable"
    with provider.batch_dates(trade_dates=days, run_id="another-own", source_revision="frozen-catalog"):
        provider.run(workspace=workspace, trade_date=days[0], historical_read_only=True)
    assert len(calls) == 2
    with pytest.raises(ValueError, match="production"):
        provider.run(workspace=workspace, trade_date=days[0])


def test_date_specific_frozen_members_are_not_reused_for_other_prefetched_days(tmp_path, monkeypatch):
    provider, workspace, days, calls = _fixture(tmp_path, monkeypatch)
    for day in days:
        (workspace.workspace_path / "manifest.json").write_text(json.dumps({"primary_assets": {}, "pit_identity": {
            "mode": "canonical_v2_frozen_candidate", "universe_as_of": day.isoformat(), "universe_codes": ["A"]}}), encoding="utf-8")
        with provider.batch_dates(trade_dates=days, run_id="own", source_revision="frozen-catalog"):
            result = provider.run(workspace=workspace, trade_date=day, historical_read_only=True)
            assert result.metadata["inference_batch_shape"] == "DATE_BOUND_FROZEN_PIT"
    assert len(calls) == 2 and [len(clocks) for clocks in calls] == [1, 1]
    assert [clocks[0].data_cutoff for clocks in calls] == list(days)


def test_source_plan_adapter_preserves_native_cutoffs_and_returns_content_receipt(tmp_path, monkeypatch):
    from backend.services.advisory_historical_range import input_source_plan as sources
    from backend.tests.advisory_historical_range.test_input_source_plan import POLICY, _payload
    provider, workspace, days, calls = _fixture(tmp_path, monkeypatch)
    plan = sources.build_source_plan(mode="DATABASE_ONLY", consumer_scope="ADVISORY", run_id="own",
        trading_calendar=["2026-06-01", "2026-06-02", "2026-06-03"], native_policy=POLICY,
        days=[{"decision_date": str(d), "data_cutoff": "2026-06-01" if i == 0 else "2026-06-02",
               "target_trade_date": str(d), "history_start": "2026-06-01"} for i, d in enumerate(days)])
    original = provider._execute
    def execute(workspace, clocks, scope):
        assert scope.source_plan == plan
        root, index = original(workspace, clocks, scope)
        path = root / "batch_dates" / str(clocks[0].decision_date) / "scores.json"
        payload = json.loads(path.read_bytes())
        route = sources.resolve_day_source(plan, clocks[0])
        content = _payload(route)
        content["metadata"].update(payload["metadata"])
        content["input_context"]["source_plan_receipt"] = sources.input_receipt(route, content)
        _atomic_json(path, content)
        index["results"][0]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        return root, index
    monkeypatch.setattr(provider, "_execute", execute)
    with provider.batch_dates(trade_dates=days, run_id="own", source_revision=plan["plan_hash"], source_plan=plan):
        result = provider.run(workspace=workspace, trade_date=days[0], cutoff_date=date(2026, 6, 1), historical_read_only=True)
        assert result.input_context["source_plan_receipt"]["route"]["plan_hash"] == plan["plan_hash"]
        assert [v.data_cutoff for v in calls[0]] == [date(2026, 6, 1), date(2026, 6, 2)]
        with pytest.raises(ValueError, match="cutoff"):
            provider.run(workspace=workspace, trade_date=days[0], cutoff_date=days[0], historical_read_only=True)
    with pytest.raises(ValueError, match="revision"):
        with provider.batch_dates(trade_dates=days, run_id="own", source_revision="wrong", source_plan=plan):
            pytest.fail("mismatched source must not run")


def test_new_identity_and_factor_code_change_do_not_reuse_wrong_native_score(tmp_path, monkeypatch):
    _, workspace, _, _ = _fixture(tmp_path, monkeypatch)
    root = workspace.workspace_path
    factor = root / "factor.py"
    factor.write_text("value = 1", encoding="utf-8")
    entry = root / "entry.py"
    entry.write_text("_FACTOR_FILES = " + repr({"new-factor-not-in-old-packages": str(factor)}), encoding="utf-8")
    manifest = {"primary_assets": {"factor_entry_relpath": "entry.py"}, "diagnostics": {"package_manifest_sha256": "a" * 64}}
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    first = _workspace_identity(root)
    # A relocated source with identical bytes is the SAME algorithm, not a
    # repeated expensive batch for every fresh native workspace nonce.
    other = root / "same_factor_other_path.py"
    other.write_bytes(factor.read_bytes())
    entry.write_text("_FACTOR_FILES = " + repr({"new-factor-not-in-old-packages": str(other)}), encoding="utf-8")
    assert _workspace_identity(root) == first
    other.write_text("value = 2", encoding="utf-8")
    assert _workspace_identity(root) != first
    changed = deepcopy(manifest)
    changed["diagnostics"]["package_manifest_sha256"] = "b" * 64
    (root / "manifest.json").write_text(json.dumps(changed), encoding="utf-8")
    assert _workspace_identity(root) != first
