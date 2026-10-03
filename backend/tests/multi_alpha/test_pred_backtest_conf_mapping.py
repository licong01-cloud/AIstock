from pathlib import Path

import pytest
import yaml

from backend.services.multi_alpha.combine_backtest import (
    MultiAlphaCombineBacktestError,
    _apply_pred_backtest_overrides_text,
)


def _conf(indent: int, newline: str = "\n") -> str:
    text = """port_analysis_config:
  strategy:
    class: TailTWAPWithLimitStrategy
    kwargs:
      topk: 50
      unfilled_handler: TAIL_BOOST
      nested:
        topk: 7
  backtest:
    start_time: 2024-07-01
    end_time: 2026-06-29
    account: 100000000
    exchange_kwargs:
      freq: 1min
task:
  model:
    class: GeneralPTNN
    kwargs:
      dimensions: {{ num_features }}
  dataset:
    class: TSDatasetH
    kwargs:
      handler:
        class: DataHandlerLP
        kwargs:
          segments:
            test: [2000-01-01, 2000-01-02]
      segments:
        train: [2018-08-01, 2022-12-30]
        valid: [2023-01-03, 2024-06-28]
        test: [2024-07-01, 2026-08-31]
  record:
    - class: SignalRecord
      kwargs:
        model: <MODEL>
        dataset: <DATASET>
"""
    head = text.split("  record:\n", 1)[0]
    rendered = newline.join(
        " " * ((len(line) - len(line.lstrip())) // 2 * indent) + line.lstrip()
        for line in head.splitlines()
    ) + newline
    # A sequence marker stays two characters wide regardless of mapping indent.
    return rendered + newline.join([
        " " * indent + "record:",
        " " * (2 * indent) + "- class: SignalRecord",
        " " * (2 * indent + 2) + "kwargs:",
        " " * (3 * indent + 2) + "model: <MODEL>",
        " " * (3 * indent + 2) + "dataset: <DATASET>",
    ]) + newline


def _apply(text: str) -> str:
    return _apply_pred_backtest_overrides_text(
        text, workspace=Path("isolated"), conf_path=Path("isolated/conf.yaml"),
        topk=20, initial_cash=100000000, oos_start="2024-07-01",
        oos_end="2026-08-28", strategy_overrides={},
    )


@pytest.mark.parametrize("indent,newline", [(2, "\n"), (4, "\n"), (4, "\r\n")])
def test_generated_task_dataset_is_distinct_from_signal_record_dataset(indent, newline):
    original = _conf(indent, newline)
    updated = _apply(original)
    assert "{{ num_features }}" in updated
    assert updated[updated.index(" " * indent + "record:"):] == original[original.index(" " * indent + "record:"):]
    parsed = yaml.safe_load(updated.replace("{{ num_features }}", "57"))
    prior = yaml.safe_load(original.replace("{{ num_features }}", "57"))
    task = parsed["task"]
    assert task["dataset"]["kwargs"]["segments"] == {
        "train": prior["task"]["dataset"]["kwargs"]["segments"]["train"],
        "valid": prior["task"]["dataset"]["kwargs"]["segments"]["valid"],
        "test": ["2024-07-01", "2026-08-28"],
    }
    assert task["dataset"]["kwargs"]["handler"]["kwargs"]["segments"]["test"][0].year == 2000
    assert task["record"][0]["kwargs"]["dataset"] == "<DATASET>"
    assert parsed["port_analysis_config"]["strategy"]["kwargs"]["topk"] == 20
    assert parsed["port_analysis_config"]["strategy"]["kwargs"]["nested"]["topk"] == 7
    assert parsed["port_analysis_config"]["backtest"]["exchange_kwargs"]["freq"] == "1min"


@pytest.mark.parametrize("change", ["duplicate_dataset", "missing_dataset", "duplicate_test"])
def test_invalid_direct_mapping_still_fails_closed(change):
    text = _conf(2)
    if change == "duplicate_dataset":
        text = text.replace("  record:", "  dataset:\n    class: DuplicateDataset\n  record:")
    elif change == "missing_dataset":
        text = text.replace("  dataset:\n", "  unrelated_dataset:\n", 1)
    else:
        text = text.replace("        test: [2024-07-01, 2026-08-31]", "        test: [2024-07-01, 2026-08-31]\n        test: [2024-07-01, 2026-08-31]")
    with pytest.raises(MultiAlphaCombineBacktestError) as caught:
        _apply(text)
    assert caught.value.reason_code == "pred_backtest_conf_invalid"
