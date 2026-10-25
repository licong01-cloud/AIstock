from __future__ import annotations

import json
import sys
from datetime import date

import pandas as pd
import pytest

from scripts import strategy_package_live_inference as runner


class _Engine:
    universe_count = 5000
    captured = None

    def __init__(self, **kwargs) -> None:
        self.last_inference_receipt = None

    def run_inference(self, **kwargs):  # noqa: ANN003, ANN201
        type(self).captured = kwargs
        self.last_inference_receipt = {
            "universe_count": self.universe_count,
            "source_read_receipts": [{"source_role": "pit_universe", "content_hash": "a" * 64}],
            "input_context": {"universe_input_hash": "b" * 64},
        }
        return pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [pd.to_datetime([]), []],
                names=["datetime", "instrument"],
            ),
            columns=["score"],
        )


def test_historical_runner_uses_explicit_read_only_and_task_diagnostic_path(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    output = tmp_path / "result" / "scores.json"
    monkeypatch.setattr(runner, "InferenceEngine", _Engine)
    monkeypatch.setattr(runner, "_patch_strategy_package_data_window", lambda: None)
    monkeypatch.setattr(runner, "_configure_temporary_root", lambda value: tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "strategy_package_live_inference.py",
            "--runtime-workspace",
            str(workspace),
            "--trade-date",
            date(2026, 6, 2).isoformat(),
            "--output-path",
            str(output),
            "--historical-read-only",
        ],
    )

    assert runner.main() == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["scores"] == []
    assert _Engine.captured["persist_signals"] is False
    assert _Engine.captured["universe_ensure"] is False
    assert _Engine.captured["allow_external_market_fallback"] is False
    assert _Engine.captured["use_selection_data_cache"] is False
    assert _Engine.captured["diagnostic_output_path"] == str(
        output.parent / "diagnostics" / "qe_diagnosis.txt"
    )


def test_historical_runner_rejects_empty_universe_as_valid_empty(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    output = tmp_path / "scores.json"
    monkeypatch.setattr(_Engine, "universe_count", 0)
    monkeypatch.setattr(runner, "InferenceEngine", _Engine)
    monkeypatch.setattr(runner, "_patch_strategy_package_data_window", lambda: None)
    monkeypatch.setattr(runner, "_configure_temporary_root", lambda value: tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "strategy_package_live_inference.py",
            "--runtime-workspace",
            str(workspace),
            "--trade-date",
            "2026-06-02",
            "--output-path",
            str(output),
            "--historical-read-only",
        ],
    )

    with pytest.raises(RuntimeError, match="empty universe"):
        runner.main()


def test_current_runner_derives_diagnostic_path_from_repo_working_directory(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    output = tmp_path / "result" / "scores.json"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(runner, "InferenceEngine", _Engine)
    monkeypatch.setattr(runner, "_patch_strategy_package_data_window", lambda: None)
    monkeypatch.setattr(runner, "_configure_temporary_root", lambda value: tmp_path)
    monkeypatch.setattr(runner, "_score_rows_from_frame", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "strategy_package_live_inference.py",
            "--runtime-workspace",
            str(workspace),
            "--trade-date",
            "2026-06-02",
            "--output-path",
            str(output),
        ],
    )

    assert runner.main() == 0
    assert _Engine.captured["diagnostic_output_path"] == str(
        tmp_path / "debug_tools" / "qe_diagnosis.txt"
    )
    assert _Engine.captured["persist_signals"] is True


def _batch_args(tmp_path, monkeypatch, plan):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "manifest.json").write_text(json.dumps({"primary_assets": {}}), encoding="utf-8")
    (workspace / "factor_order.json").write_text("[]", encoding="utf-8")
    from backend.services.advisory_historical_range.inference_inputs import InferenceInputSession
    monkeypatch.setattr(runner, "database_input_session", lambda **kwargs: InferenceInputSession(**kwargs))
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    output = tmp_path / "batch.json"
    monkeypatch.setattr(runner, "_configure_temporary_root", lambda value: tmp_path)
    monkeypatch.setattr(runner, "_patch_strategy_package_data_window", lambda: None)
    monkeypatch.setattr(sys, "argv", [
        "strategy_package_live_inference.py", "--runtime-workspace", str(workspace),
        "--historical-plan", str(plan_path), "--historical-read-only", "--temp-root", str(tmp_path),
        "--output-path", str(output), "--run-id", "test-owned-run", "--input-revision", "fixture-immutable-v1",
    ])
    return output


def test_batch_runner_reuses_one_session_and_keeps_full_daily_receipts(tmp_path, monkeypatch):
    output = _batch_args(tmp_path, monkeypatch, [
        {"trade_date": "2026-06-02", "cutoff_date": "2026-06-01"},
        {"trade_date": "2026-06-03", "cutoff_date": "2026-06-02"},
    ])
    sessions, requests = [], []

    class BatchEngine(_Engine):
        def __init__(self, *, resource_session, **kwargs):
            super().__init__()
            sessions.append(resource_session)

        def run_inference(self, **kwargs):
            super().run_inference(**kwargs)
            requests.append(kwargs)
            return pd.DataFrame({"score": [1.0, 2.0]}, index=pd.MultiIndex.from_product(
                [[pd.Timestamp(kwargs["cutoff_date"])], ["SH600001", "SZ000002"]], names=["datetime", "instrument"],
            ))

    monkeypatch.setattr(runner, "InferenceEngine", BatchEngine)
    assert runner.main() == 0
    index = json.loads(output.read_text(encoding="utf-8"))
    assert index["ok"] is True and index["status"] == "COMPLETE"
    assert len(sessions) == 1 and sessions[0].cache_bytes == 0
    assert [request["trade_date"].date().isoformat() for request in requests] == ["2026-06-02", "2026-06-03"]
    for request, result in zip(requests, index["results"], strict=True):
        assert all(request[key] is False for key in ("persist_signals", "universe_ensure", "allow_external_market_fallback", "use_selection_data_cache"))
        payload = json.loads(runner.Path(result["output_path"]).read_text(encoding="utf-8"))
        assert payload["source_read_receipts"] == [{"source_role": "pit_universe", "content_hash": "a" * 64}]
        assert payload["input_context"] == {"universe_input_hash": "b" * 64}
        assert [row["symbol"] for row in payload["scores"]] == ["SZ000002", "SH600001"]


def test_batch_failure_stops_dates_and_does_not_publish_complete(tmp_path, monkeypatch):
    output = _batch_args(tmp_path, monkeypatch, [{"trade_date": day} for day in ("2026-06-02", "2026-06-03", "2026-06-04")])
    requests = []

    class FailingEngine(_Engine):
        def __init__(self, *, resource_session, **kwargs):
            super().__init__()

        def run_inference(self, **kwargs):
            requests.append(kwargs["trade_date"].date().isoformat())
            if len(requests) == 2:
                raise ValueError("unavailable historical input")
            return super().run_inference(**kwargs)

    monkeypatch.setattr(runner, "InferenceEngine", FailingEngine)
    with pytest.raises(ValueError, match="unavailable historical input"):
        runner.main()
    index = json.loads(output.read_text(encoding="utf-8"))
    assert index["ok"] is False and index["status"] == "FAILED"
    assert len(index["results"]) == 1 and requests == ["2026-06-02", "2026-06-03"]


def test_exact_resume_handles_publish_before_commit_and_rejects_foreign_or_changed_input(tmp_path, monkeypatch):
    output = _batch_args(tmp_path, monkeypatch, [{"trade_date": "2026-06-02"}, {"trade_date": "2026-06-03"}])
    calls, written = [], []
    class Engine(_Engine):
        def run_inference(self, **kwargs):
            calls.append(kwargs["trade_date"].date())
            return super().run_inference(**kwargs)
    monkeypatch.setattr(runner, "InferenceEngine", Engine)
    native_write = runner._atomic_json
    def crash(path, payload):
        if path.name == "commit.json" and not written:
            written.append(path)
            raise OSError("publish-before-commit interruption")
        native_write(path, payload)
    monkeypatch.setattr(runner, "_atomic_json", crash)
    with pytest.raises(OSError, match="interruption"):
        runner.main()
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "FAILED"
    monkeypatch.setattr(runner, "_atomic_json", native_write)
    monkeypatch.setattr(sys, "argv", sys.argv + ["--resume"])
    assert runner.main() == 0
    assert calls == [date(2026, 6, 2), date(2026, 6, 3)]  # No duplicate model invocation/publication.
    assert runner.main() == 0 and len(calls) == 2
    original = list(sys.argv)
    monkeypatch.setattr(sys, "argv", original + ["--consumer-scope", "LOCALSIM"])
    with pytest.raises(ValueError, match="identity changed"):
        runner.main()
    # Change the source revision, not the old completed result.
    changed = original.copy()
    changed[changed.index("--input-revision") + 1] = "changed-source-v2"
    monkeypatch.setattr(sys, "argv", changed)
    with pytest.raises(ValueError, match="identity changed"):
        runner.main()


def test_atomic_output_error_does_not_leave_or_accept_temporary_success(tmp_path):
    with pytest.raises(ValueError):
        runner._atomic_json(tmp_path / "output.json", {"score": float("nan")})
    assert not (tmp_path / "output.json").exists() and not list(tmp_path.glob(".inference-*"))


def test_multi_model_same_date_has_one_engine_session_and_independent_complete_outputs(tmp_path, monkeypatch):
    output = _batch_args(tmp_path, monkeypatch, [
        {"trade_date": "2026-06-02", "request_id": "new-leg-A"},
        {"trade_date": "2026-06-02", "request_id": "new-package-B"},
        {"trade_date": "2026-06-03", "request_id": "new-leg-A"},
    ])
    engines = []
    class Engine(_Engine):
        def __init__(self, **kwargs):
            super().__init__()
            engines.append(kwargs)
    monkeypatch.setattr(runner, "InferenceEngine", Engine)
    assert runner.main() == 0 and len(engines) == 1
    index = json.loads(output.read_text(encoding="utf-8"))
    assert [row["request_id"] for row in index["results"]] == ["new-leg-A", "new-package-B", "new-leg-A"]
    assert len({row["output_path"] for row in index["results"]}) == 3
    monkeypatch.setattr(sys, "argv", sys.argv + ["--resume"])
    assert runner.main() == 0
    assert len(list((tmp_path / "batch_dates").rglob("scores.json"))) == 3


@pytest.mark.parametrize("plan", [
    [{"trade_date": "2026-06-02", "cutoff_date": "2026-06-03"}],
    [{"trade_date": "2026-06-02"}, {"trade_date": "2026-06-02"}],
    [{"trade_date": "2026-06-03"}, {"trade_date": "2026-06-02"}],
])
def test_batch_plan_preserves_cutoff_unique_dates_and_order(tmp_path, monkeypatch, plan):
    output = _batch_args(tmp_path, monkeypatch, plan)
    with pytest.raises(ValueError):
        runner.main()
    assert not output.exists()


def test_temporary_root_never_accepts_c_drive():
    with pytest.raises(ValueError, match="temporary root"):
        runner._configure_temporary_root("C:/AIstock_temp/advisory")
