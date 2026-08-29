from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from scripts import nightly_session_runner as runner


def test_run_sessions_checkpoints_each_result_and_continues_after_failure(tmp_path: Path) -> None:
    output_json = tmp_path / "session-results.json"
    output_md = tmp_path / "session-results.md"
    calls: list[str] = []

    def fake_executor(session: str, timeout_seconds: int, positional_args: list[str] | None):
        calls.append(session)
        assert positional_args == []
        if session == "second":
            checkpoint = json.loads(output_json.read_text(encoding="utf-8"))
            assert [row["session"] for row in checkpoint] == ["first"]
        return {
            "session": session,
            "result": "failure" if session == "second" else "success",
            "failure_kind": "nonzero_exit" if session == "second" else None,
            "return_code": 1 if session == "second" else 0,
            "duration_seconds": 0.1,
            "timeout_seconds": timeout_seconds,
        }

    results = runner.run_sessions(
        ["first", "second", "third"],
        output_json=output_json,
        output_md=output_md,
        session_timeout_seconds=60,
        total_timeout_seconds=180,
        executor=fake_executor,
    )

    assert calls == ["first", "second", "third"]
    assert [row["result"] for row in results] == ["success", "failure", "success"]
    assert json.loads(output_json.read_text(encoding="utf-8")) == results
    assert "`second` | `failure` | `nonzero_exit`" in output_md.read_text(encoding="utf-8")


def test_execute_session_returns_explicit_timeout(monkeypatch) -> None:
    monkeypatch.setattr(
        runner,
        "sys",
        type("FakeSys", (), {"executable": sys.executable})(),
    )
    original_popen = runner.subprocess.Popen

    def sleeping_popen(command, **kwargs):
        if command and command[0] == sys.executable:
            return original_popen([sys.executable, "-c", "import time; time.sleep(5)"], **kwargs)
        # subprocess.run(taskkill ...) also delegates to subprocess.Popen.
        # Preserve that real termination path instead of replacing it with a
        # second sleeper and manufacturing a timeout overrun.
        return original_popen(command, **kwargs)

    monkeypatch.setattr(runner.subprocess, "Popen", sleeping_popen)

    started = time.monotonic()
    result = runner.execute_session("hung_session", 1)

    assert time.monotonic() - started < 4
    assert result["result"] == "failure"
    assert result["failure_kind"] == "timeout"
    assert result["return_code"] is None


def test_execute_session_uses_scope_file_instead_of_long_command_line(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class CompletedProcess:
        def wait(self, timeout: int) -> int:
            captured["timeout"] = timeout
            return 0

    def fake_popen(command, **kwargs):
        captured["command"] = command
        scope_path = Path(kwargs["env"]["AISTOCK_NIGHTLY_SESSION_ARGS_FILE"])
        captured["scope_path"] = scope_path
        captured["scope"] = json.loads(scope_path.read_text(encoding="utf-8"))
        return CompletedProcess()

    monkeypatch.setattr(runner.subprocess, "Popen", fake_popen)
    paths = [f"backend/tests/test_{index}.py" for index in range(1500)]

    result = runner.execute_session("l0", 60, paths)

    assert captured["command"] == [sys.executable, "-m", "nox", "-s", "l0"]
    assert captured["scope"] == paths
    assert not captured["scope_path"].exists()  # type: ignore[union-attr]
    assert result["positional_arg_count"] == 1500


def test_invalid_plan_writes_failure_receipt(tmp_path: Path) -> None:
    plan = tmp_path / "plan.json"
    output_json = tmp_path / "results.json"
    output_md = tmp_path / "results.md"
    plan.write_text("{}", encoding="utf-8")

    exit_code = runner.main(
        [
            "--plan",
            str(plan),
            "--output-json",
            str(output_json),
            "--output-md",
            str(output_md),
        ]
    )

    assert exit_code == 2
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload[0]["result"] == "failure"
    assert payload[0]["failure_kind"] == "invalid_plan"


def test_execution_plan_passes_changed_files_only_to_change_scoped_l0(tmp_path: Path, monkeypatch) -> None:
    plan = tmp_path / "plan.json"
    output_json = tmp_path / "results.json"
    output_md = tmp_path / "results.md"
    plan.write_text(
        json.dumps(
            {
                "selected_sessions": ["l0", "validation_module_registry_l0"],
                "changed_files": ["scripts/nightly_session_runner.py", "backend/tests/test_example.py"],
            }
        ),
        encoding="utf-8",
    )
    calls: list[tuple[str, list[str] | None]] = []

    def fake_run_sessions(sessions: list[str], **kwargs):
        assert sessions == ["l0", "validation_module_registry_l0"]
        calls.extend((session, kwargs["session_args"].get(session)) for session in sessions)
        return []

    monkeypatch.setattr(runner, "run_sessions", fake_run_sessions)

    assert runner.main(["--plan", str(plan), "--output-json", str(output_json), "--output-md", str(output_md)]) == 0
    assert calls == [
        ("l0", ["scripts/nightly_session_runner.py", "backend/tests/test_example.py"]),
        ("validation_module_registry_l0", None),
    ]


def test_change_scoped_l0_without_changed_files_is_invalid(tmp_path: Path) -> None:
    plan = tmp_path / "plan.json"
    output_json = tmp_path / "results.json"
    output_md = tmp_path / "results.md"
    plan.write_text(json.dumps({"selected_sessions": ["l0"], "changed_files": []}), encoding="utf-8")

    exit_code = runner.main(
        ["--plan", str(plan), "--output-json", str(output_json), "--output-md", str(output_md)]
    )

    assert exit_code == 2
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert "change-scoped sessions without changed_files" in payload[0]["error"]


def test_nightly_workflow_uses_checkpointed_runner_and_bounds_optional_llm_failures() -> None:
    workflow = (runner.Path(__file__).resolve().parents[3] / ".github" / "workflows" / "nightly.yml").read_text(
        encoding="utf-8"
    )

    assert "scripts/nightly_session_runner.py" in workflow
    assert "--session-timeout-seconds 1200" in workflow
    assert "--total-timeout-seconds 6300" in workflow
    assert "id: nightly_discovery_hypotheses" in workflow
    assert "id: nightly_llm_outcomes" in workflow
