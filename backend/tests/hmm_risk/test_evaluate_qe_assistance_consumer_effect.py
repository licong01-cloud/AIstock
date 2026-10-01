from __future__ import annotations

import json
import sys

from scripts.hmm_risk import evaluate_qe_assistance_consumer_effect as cli


def test_cli_writes_only_compact_label_free_result(tmp_path, monkeypatch) -> None:
    output = tmp_path / "consumer-effect.json"
    result = {
        "status": "consumer_effect_observed",
        "result_sha256": "a" * 64,
        "changed_score_count": 12,
        "rank_changed_row_count": 4,
        "topk_changed_date_count": 2,
        "tail_accessed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    monkeypatch.setattr(cli, "load_calendar", lambda _path: ["2024-07-01", "2024-07-02"])
    monkeypatch.setattr(cli, "load_prediction_rows", lambda _path, _calendar: [{"row": 1}])
    monkeypatch.setattr(cli, "load_json_object", lambda _path: {"artifact": 1})
    monkeypatch.setattr(cli, "evaluate_consumer_effect", lambda **_kwargs: result)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "evaluate_qe_assistance_consumer_effect.py",
            "--prediction-pickle",
            str(tmp_path / "pred.pkl"),
            "--calendar",
            str(tmp_path / "calendar.json"),
            "--artifact",
            str(tmp_path / "artifact.json"),
            "--output",
            str(output),
        ],
    )

    assert cli.main() == 0
    assert json.loads(output.read_text(encoding="utf-8")) == result
