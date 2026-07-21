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

    def __init__(self) -> None:
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
