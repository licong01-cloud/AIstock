from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "hmm_risk" / "prepare_qe_assistance_three_arm.py"


def _load():
    spec = importlib.util.spec_from_file_location("prepare_qe_assistance_three_arm", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_write_new_is_exclusive_and_durable_json(tmp_path: Path) -> None:
    subject = _load()
    target = tmp_path / "request.json"
    subject._write_new(target, {"value": 1})
    assert json.loads(target.read_text(encoding="utf-8")) == {"value": 1}
    with pytest.raises(FileExistsError):
        subject._write_new(target, {"value": 2})


def test_fetch_source_config_requires_nested_config(monkeypatch) -> None:
    subject = _load()

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(subject.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())
    monkeypatch.setattr(subject.json, "load", lambda _response: {"data": {"config_json": {"model_id": "m"}}})
    assert subject._fetch_source_config("http://example.test", 1) == {"model_id": "m"}


def test_submission_preflight_rejects_future_filtered_pool_snapshot() -> None:
    subject = _load()
    request = {
        "loops": [
            {
                "stock_pool": "filtered_pool_20260502",
                "data_split": {"test_end": "2026-03-31"},
            }
            for _index in range(3)
        ]
    }

    with pytest.raises(RuntimeError, match="QE_STOCK_POOL_DATE_OUT_OF_WINDOW"):
        subject._validate_submission_preflight(request)


def test_submission_preflight_accepts_pool_not_after_formal_window() -> None:
    subject = _load()
    request = {
        "loops": [
            {
                "stock_pool": "filtered_pool_20260331",
                "data_split": {"test_end": "2026-03-31"},
            }
            for _index in range(3)
        ]
    }

    subject._validate_submission_preflight(request)
