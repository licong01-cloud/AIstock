from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest

from scripts import dataset_release_candidate_consumer_smoke as script


def test_consumer_result_is_create_once_and_never_overwrites(tmp_path: Path) -> None:
    result = tmp_path / "attempt" / "semantic_result.json"
    script._atomic_json(result, {"generation": 1})

    with pytest.raises(ValueError, match="already exists"):
        script._atomic_json(result, {"generation": 2})

    assert json.loads(result.read_text(encoding="utf-8")) == {"generation": 1}


def test_consumer_path_binding_rejects_sibling_or_escape(tmp_path: Path) -> None:
    expected = tmp_path / "candidate" / ".staging" / "release" / "daily_bin"
    sibling = tmp_path / "candidate" / ".staging" / "other" / "daily_bin"
    expected.mkdir(parents=True)
    sibling.mkdir(parents=True)

    with pytest.raises(ValueError, match="fenced identity"):
        script._assert_exact_path(str(sibling), expected, must_exist=True)


def test_consumer_run_uses_attempt_fenced_staging_not_release_id(monkeypatch, tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    control = tmp_path / "control"
    staging = candidate / ".staging" / "attempt-1" / "7"
    daily = staging / "daily_bin" / "qlib"
    minute = staging / "minute_bin" / "qlib"
    index_h5 = staging / "index_context" / "index_daily.h5"
    daily.mkdir(parents=True)
    minute.mkdir(parents=True)
    index_h5.parent.mkdir(parents=True)
    index_h5.write_bytes(b"index")
    result = control / "attempt_runs" / "attempt-1-7" / "build-consumer-smoke" / "semantic_result.json"
    result.parent.mkdir(parents=True)
    observed: dict[str, object] = {}

    def fake_smoke(spec, *, checkpoint):
        observed["spec"] = spec
        checkpoint()
        return {"status": "PASS"}

    monkeypatch.setattr(script, "run_candidate_consumer_smoke", fake_smoke)
    monkeypatch.setattr(
        script,
        "ChildResourceCheckpoint",
        lambda **_kwargs: Namespace(checkpoint=lambda: None),
    )
    monkeypatch.setattr(
        script,
        "CandidateConsumerSmokeSpec",
        lambda **kwargs: Namespace(**kwargs),
    )
    args = Namespace(
        control_root=str(control),
        candidate_root=str(candidate),
        daily_provider_uri=str(daily),
        minute_provider_uri=str(minute),
        index_h5_path=str(index_h5),
        cutoff="2026-07-31",
        stock_instrument="000001.SZ",
        profile="qe_hmm_full_v1",
        run_id="run-1",
        attempt_id="attempt-1",
        attempt_fence=7,
        release_id="release-different-from-attempt",
        release_digest="a" * 64,
        staging_relative_path=".staging/attempt-1/7",
        execution_id="build-consumer-smoke",
        max_h5_rows=100,
        stage_timeout_seconds=43200,
        result_path=str(result),
    )

    assert script._run(args) == 0
    assert result.exists()
    assert observed["spec"].staging_relative_path == ".staging/attempt-1/7"

    bad = Namespace(
        **{
            **vars(args),
            "staging_relative_path": (".staging/release-different-from-attempt"),
            "result_path": str(result.with_name("bad-result.json")),
        }
    )
    with pytest.raises(ValueError, match="staging relative identity differs"):
        script._run(bad)


def test_consumer_top_level_error_never_logs_exception_text_or_traceback(monkeypatch, capsys) -> None:
    sensitive_text = "SENSITIVE_VALUE=" + "must-not-be-persisted"

    def sensitive_failure(_args):
        raise RuntimeError(sensitive_text)

    monkeypatch.setattr(script, "_run", sensitive_failure)
    code = script.main(
        [
            "--daily-provider-uri",
            "/candidate/day",
            "--minute-provider-uri",
            "/candidate/minute",
            "--index-h5-path",
            "/candidate/index.h5",
            "--cutoff",
            "2026-07-31",
            "--stock-instrument",
            "000001.SZ",
            "--profile",
            "qe_hmm_full_v1",
            "--run-id",
            "run-1",
            "--attempt-id",
            "attempt-1",
            "--attempt-fence",
            "1",
            "--release-id",
            "release-1",
            "--release-digest",
            "a" * 64,
            "--staging-relative-path",
            ".staging/release-1",
            "--execution-id",
            "build-consumer-smoke",
            "--max-h5-rows",
            "100",
            "--stage-timeout-seconds",
            "43200",
            "--result-path",
            "/control/result.json",
            "--control-root",
            "/control",
            "--candidate-root",
            "/candidate",
        ]
    )
    error = capsys.readouterr().err

    assert code == 2
    assert sensitive_text not in error
    assert "Traceback" not in error
    assert json.loads(error)["error_code"] == "BLOCKED_CANDIDATE_CONSUMER_SMOKE"
